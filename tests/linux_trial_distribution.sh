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
Usage: tests/linux_trial_distribution.sh --archive FILE --verifier FILE \
  --installer FILE [--source-commit SHA]

Exercise checksum rejection, immutable installation and explicit quiesced
activation of an actual Ubuntu 24.04 x86_64 trial archive. The verifier and
installer paths may come from the extracted archive under test.
EOF
}

fail() {
  printf 'linux-trial-distribution: %s\n' "$*" >&2
  exit 1
}

archive=
verifier=
installer=
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
    --verifier)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      verifier=$2
      shift 2
      ;;
    --installer)
      (($# >= 2)) || {
        usage >&2
        exit 2
      }
      installer=$2
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

[[ -n $archive && -n $verifier && -n $installer ]] || {
  usage >&2
  exit 2
}
[[ -f $archive ]] || fail "archive is not a file: $archive"
[[ -x $verifier ]] || fail "verifier is not executable: $verifier"
[[ -x $installer ]] || fail "installer is not executable: $installer"
for command in awk cp ln mkdir mktemp readlink rm sed sha256sum unlink; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done

archive=$(cd "$(dirname "$archive")" && pwd -P)/$(basename "$archive")
verifier=$(cd "$(dirname "$verifier")" && pwd -P)/$(basename "$verifier")
installer=$(cd "$(dirname "$installer")" && pwd -P)/$(basename "$installer")
if [[ -n $source_commit ]]; then
  verification=$("$verifier" --archive "$archive" --source-commit "$source_commit")
else
  verification=$("$verifier" --archive "$archive")
fi
printf '%s\n' "$verification"

release=$(awk -F= '$1 == "release" { print $2 }' <<<"$verification")
version=$(awk -F= '$1 == "version" { print $2 }' <<<"$verification")
source=$(awk -F= '$1 == "source_commit" { print $2 }' <<<"$verification")
[[ -n $release ]] || fail "verification did not report a release"
if [[ -z $version ]]; then
  version=${release%%+*}
fi
[[ -n $source ]] || fail "verification did not report a source commit"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
prefix="$work/prefix"

mkdir -p "$prefix"
ln -s missing-release "$prefix/$release"
if "$installer" install --archive "$archive" --prefix "$prefix" >/dev/null 2>&1; then
  fail "installer followed a dangling release symlink"
fi
unlink "$prefix/$release"

install_output=$("$installer" install --archive "$archive" --prefix "$prefix")
printf '%s\n' "$install_output"
installed=$(awk -F= '$1 == "installed_release" { print $2 }' <<<"$install_output")
installed_path=$(awk -F= '$1 == "installed_path" { print $2 }' <<<"$install_output")
[[ $installed == "$release" ]] || fail "installer reported an unexpected release"
[[ -x "$installed_path/bin/yy" ]] || fail "installed archive binary is missing"
[[ ! -e "$prefix/current" ]] || fail "stage-only installation changed current"

if "$installer" install --archive "$archive" --prefix "$prefix" >/dev/null 2>&1; then
  fail "installer overwrote an existing immutable release"
fi

old_source=0000000000000000000000000000000000000000
old_release="$version+${old_source:0:12}"
old_path="$prefix/$old_release"
cp -a "$installed_path" "$old_path"
sed -i \
  -e "s/^release=.*/release=$old_release/" \
  -e "s/^source_commit=.*/source_commit=$old_source/" \
  "$old_path/BUILD-MANIFEST"
ln -s "$old_release" "$prefix/current"

if "$installer" activate --prefix "$prefix" --release "$release" >/dev/null 2>&1; then
  fail "activation succeeded without the quiesced assertion"
fi

activation=$("$installer" activate --prefix "$prefix" --release "$release" --quiesced)
printf '%s\n' "$activation"
[[ $(readlink "$prefix/current") == "$release" ]] ||
  fail "activation did not select the trial release"
[[ ! -e "$prefix/previous" ]] || fail "activation created a hidden rollback pointer"
if "$installer" rollback --prefix "$prefix" --quiesced >/dev/null 2>&1; then
  fail "installer exposed an unsupported state downgrade command"
fi
[[ -d $old_path ]] || fail "activation deleted the prior immutable release"
inventory=$("$installer" inspect --prefix "$prefix")
printf '%s\n' "$inventory"
grep -q '^scope=installed-release-directories-only$' <<<"$inventory" ||
  fail "release inventory overstates its accounting scope"
grep -q '^retention=manual-no-deletion$' <<<"$inventory" ||
  fail "release inventory does not state the retention boundary"
grep -q "^current=$release$" <<<"$inventory" ||
  fail "release inventory does not report the active release"
grep -q '^retained_releases=2$' <<<"$inventory" ||
  fail "release inventory did not retain both release directories"

tampered="$work/tampered"
mkdir -p "$tampered"
cp "$archive" "$tampered/$(basename "$archive")"
cp "$archive.sha256" "$tampered/$(basename "$archive").sha256"
printf 'tampered\n' >>"$tampered/$(basename "$archive")"
if "$verifier" --archive "$tampered/$(basename "$archive")" >/dev/null 2>&1; then
  fail "verifier accepted an archive whose checksum changed"
fi

printf 'linux-trial-distribution: pass\n'
