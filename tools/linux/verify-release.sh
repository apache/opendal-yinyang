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
Usage: verify-release.sh --archive FILE [--checksum FILE] [--source-commit SHA]

Verify a local Ubuntu 24.04 x86_64 YinYang trial archive without installing or
publishing it. The adjacent .sha256 file is used by default. This verifies the
archive inventory, checksums, build manifest and executable identity. It does
not establish a signature, ASF release vote or artifact authenticity.
EOF
}

die() {
  printf 'verify-release: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

manifest_value() {
  local key=$1
  local count
  count=$(awk -F= -v key="$key" '$1 == key { count++ } END { print count + 0 }' "$manifest")
  [[ $count == 1 ]] || die "BUILD-MANIFEST must contain exactly one $key field"
  awk -F= -v key="$key" '
    $1 == key {
      sub(/^[^=]*=/, "")
      print
      exit
    }
  ' "$manifest"
}

require_sha256() {
  [[ $1 =~ ^[0-9a-f]{64}$ ]] || die "$2 is not a lowercase SHA-256 digest"
}

archive=
checksum=
source_commit=
while (($#)); do
  case "$1" in
    --archive)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      archive=$2
      shift 2
      ;;
    --checksum)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      checksum=$2
      shift 2
      ;;
    --source-commit)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      source_commit=$2
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

[[ -n $archive ]] || {
  usage >&2
  exit 2
}
[[ -f $archive ]] || die "archive is not a file: $archive"
for command in awk basename diff dirname du grep gzip mktemp sha256sum sort tar wc; do
  require_command "$command"
done

archive=$(cd "$(dirname "$archive")" && pwd -P)/$(basename "$archive")
archive_name=$(basename "$archive")
[[ $archive_name == *.tar.gz ]] || die "archive name must end in .tar.gz"
artifact_root=${archive_name%.tar.gz}
[[ $artifact_root =~ ^yinyang-[0-9A-Za-z.+_-]+-ubuntu-24\.04-x86_64-[0-9a-f]{12}$ ]] ||
  die "unexpected archive name: $archive_name"

if [[ -z $checksum ]]; then
  checksum="$archive.sha256"
fi
[[ -f $checksum ]] || die "checksum file is missing: $checksum"
checksum_line=$(
  awk 'NF { count++; value = $0 } END { if (count == 1) print value; else exit 1 }' "$checksum"
) || die "checksum file must contain exactly one non-empty line"
read -r expected_archive_sha256 checksum_name _ <<<"$checksum_line"
require_sha256 "$expected_archive_sha256" "archive checksum"
[[ ${checksum_name:-} == "$archive_name" ]] ||
  die "checksum names $checksum_name, expected $archive_name"
actual_archive_sha256=$(sha256sum "$archive" | awk '{print $1}')
[[ $actual_archive_sha256 == "$expected_archive_sha256" ]] ||
  die "archive SHA-256 does not match $checksum"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
listing="$work/listing"
verbose_listing="$work/verbose-listing"
tar -tzf "$archive" >"$listing"
tar -tvzf "$archive" >"$verbose_listing"

[[ -s $listing ]] || die "archive is empty"
duplicates=$(sort "$listing" | uniq -d)
[[ -z $duplicates ]] || die "archive contains duplicate entries: $duplicates"
sort -o "$listing" "$listing"

unsafe=$(
  awk -v root="$artifact_root" '
    {
      if ($0 ~ /^\// || $0 ~ /(^|\/)\.\.(\/|$)/ || $0 ~ /\\/) {
        print
        bad = 1
        next
      }
      split($0, parts, "/")
      if (parts[1] != root) {
        print
        bad = 1
      }
    }
    END { exit bad }
  ' "$listing" || true
)
[[ -z $unsafe ]] || die "archive contains entries outside $artifact_root or unsafe paths: $unsafe"

unsupported_type=$(
  awk 'substr($0, 1, 1) != "-" && substr($0, 1, 1) != "d" { print }' "$verbose_listing"
)
[[ -z $unsupported_type ]] ||
  die "archive contains unsupported entry types: $unsupported_type"

expected_listing="$work/expected-listing"
cat >"$expected_listing" <<EOF
$artifact_root/
$artifact_root/BUILD-MANIFEST
$artifact_root/INSTALL
$artifact_root/LICENSE
$artifact_root/NOTICE
$artifact_root/PREREQUISITES
$artifact_root/bin/
$artifact_root/bin/yy
$artifact_root/lib/
$artifact_root/lib/systemd/
$artifact_root/lib/systemd/user/
$artifact_root/lib/systemd/user/yinyang-mount@.service
$artifact_root/lib/systemd/user/yinyang-serve.service
$artifact_root/lib/systemd/user/yinyang-sync@.service
$artifact_root/share/
$artifact_root/share/doc/
$artifact_root/share/doc/yinyang/
$artifact_root/share/doc/yinyang/linux-backup-recovery.md
$artifact_root/share/doc/yinyang/linux-compatibility-acceptance.md
$artifact_root/share/doc/yinyang/linux-operating-envelope.md
$artifact_root/share/doc/yinyang/linux-operations.md
$artifact_root/share/doc/yinyang/linux-qualification.md
$artifact_root/share/doc/yinyang/linux-trial-distribution.md
$artifact_root/share/yinyang/
$artifact_root/share/yinyang/environment.example
$artifact_root/share/yinyang/install-release.sh
$artifact_root/share/yinyang/verify-release.sh
EOF
sort -o "$expected_listing" "$expected_listing"
diff -u "$expected_listing" "$listing" ||
  die "archive inventory differs from the required trial distribution"

extract="$work/extract"
mkdir -p "$extract"
tar -xzf "$archive" --no-same-owner --no-same-permissions -C "$extract"
tree="$extract/$artifact_root"
[[ -d $tree ]] || die "archive root is missing after extraction"

manifest="$tree/BUILD-MANIFEST"
[[ -f $manifest ]] || die "BUILD-MANIFEST is missing"
manifest_keys=$(awk -F= '{ print $1 }' "$manifest" | sort)
expected_manifest_keys=$(printf '%s\n' \
  cargo \
  package \
  release \
  rustc \
  schema \
  source_commit \
  source_date_epoch \
  target \
  version \
  yy_sha256 | sort)
[[ $manifest_keys == "$expected_manifest_keys" ]] ||
  die "BUILD-MANIFEST contains unexpected or missing fields"

schema=$(manifest_value schema)
package=$(manifest_value package)
version=$(manifest_value version)
release=$(manifest_value release)
manifest_source=$(manifest_value source_commit)
source_date_epoch=$(manifest_value source_date_epoch)
target=$(manifest_value target)
expected_binary_sha256=$(manifest_value yy_sha256)

[[ $schema == yinyang-linux-release-artifact.v1 ]] || die "unsupported manifest schema: $schema"
[[ $package == yinyang ]] || die "unexpected package: $package"
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] ||
  die "invalid package version: $version"
[[ $manifest_source =~ ^[0-9a-f]{40}$ ]] || die "source_commit is not a Git SHA-1"
[[ $source_date_epoch =~ ^[0-9]+$ ]] || die "source_date_epoch is not an integer"
[[ $target == ubuntu-24.04-x86_64 ]] || die "unsupported target: $target"
require_sha256 "$expected_binary_sha256" "yy_sha256"

source_short=${manifest_source:0:12}
[[ $release == "$version+$source_short" ]] ||
  die "release identity does not match the package version and source"
expected_artifact_root="yinyang-$version-ubuntu-24.04-x86_64-$source_short"
[[ $artifact_root == "$expected_artifact_root" ]] ||
  die "archive name does not match BUILD-MANIFEST"
if [[ -n $source_commit ]]; then
  [[ $manifest_source == "$source_commit" ]] ||
    die "source_commit $manifest_source does not match expected $source_commit"
fi

binary="$tree/bin/yy"
[[ -f $binary && -x $binary ]] || die "bin/yy is missing or not executable"
actual_binary_sha256=$(sha256sum "$binary" | awk '{print $1}')
[[ $actual_binary_sha256 == "$expected_binary_sha256" ]] ||
  die "bin/yy SHA-256 does not match BUILD-MANIFEST"
"$binary" --help >/dev/null || die "packaged bin/yy cannot execute"

[[ -s "$tree/LICENSE" ]] || die "LICENSE is missing or empty"
grep -q 'Apache License' "$tree/LICENSE" || die "LICENSE is not the Apache License"
[[ -s "$tree/NOTICE" ]] || die "NOTICE is missing or empty"
[[ -x "$tree/share/yinyang/install-release.sh" ]] || die "install-release.sh is not executable"
[[ -x "$tree/share/yinyang/verify-release.sh" ]] || die "verify-release.sh is not executable"

prerequisites="$tree/PREREQUISITES"
[[ -f $prerequisites ]] || die "PREREQUISITES is missing"
for key in schema release source_commit target; do
  count=$(awk -F= -v key="$key" '$1 == key { count++ } END { print count + 0 }' "$prerequisites")
  [[ $count == 1 ]] || die "PREREQUISITES must contain exactly one $key field"
done
[[ $(awk -F= '$1 == "schema" { print $2 }' "$prerequisites") == yinyang-linux-release-prerequisites.v1 ]] ||
  die "unsupported PREREQUISITES schema"
[[ $(awk -F= '$1 == "release" { print $2 }' "$prerequisites") == "$release" ]] ||
  die "PREREQUISITES release does not match BUILD-MANIFEST"
[[ $(awk -F= '$1 == "source_commit" { print $2 }' "$prerequisites") == "$manifest_source" ]] ||
  die "PREREQUISITES source does not match BUILD-MANIFEST"
[[ $(awk -F= '$1 == "target" { print $2 }' "$prerequisites") == "$target" ]] ||
  die "PREREQUISITES target does not match BUILD-MANIFEST"

archive_bytes=$(wc -c <"$archive" | awk '{print $1}')
release_bytes=$(du -sb "$tree" | awk '{print $1}')
printf 'schema=yinyang-linux-release-verification.v1\n'
printf 'status=pass\n'
printf 'archive=%s\n' "$archive"
printf 'archive_sha256=%s\n' "$actual_archive_sha256"
printf 'archive_bytes=%s\n' "$archive_bytes"
printf 'artifact_root=%s\n' "$artifact_root"
printf 'release=%s\n' "$release"
printf 'source_commit=%s\n' "$manifest_source"
printf 'target=%s\n' "$target"
printf 'yy_sha256=%s\n' "$actual_binary_sha256"
printf 'release_bytes=%s\n' "$release_bytes"
printf 'checksum_status=verified\n'
printf 'inventory_status=verified\n'
printf 'binary_help=pass\n'
