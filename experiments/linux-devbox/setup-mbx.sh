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

# Task-only devbox tooling; not part of the product stack.
set -euo pipefail
umask 077
tool_dir="$HOME/.local/lib/yinyang-devbox-tools/mbx-1.28.0-mise-2026.10.20"
mkdir -p "$tool_dir" "$HOME/.local/bin"
cd "$tool_dir"

download() {
    local url=$1 artifact=$2 digest=$3
    if [[ ! -f "$artifact" ]]; then
        curl --fail --location --retry 2 --output "$artifact.tmp" "$url"
        printf '%s  %s\n' "$digest" "$artifact.tmp" | sha256sum --check
        mv "$artifact.tmp" "$artifact"
    fi
    printf '%s  %s\n' "$digest" "$artifact" | sha256sum --check
}

download https://github.com/jdx/mr-boxington/releases/download/v1.28.0/mbx-x86_64-unknown-linux-gnu.tar.gz mbx.tar.gz 50ce9360221d9274a73d70c9d2d5285dc2d8353a101de0845e3da5bb0083bcfb
download https://github.com/jdx/mise/releases/download/v2026.10.20/mise-v2026.10.20-linux-x64 mise 2eb27467b6f369915ee76f18d66a0696cdb5c20e7ecc8cb49c2bfaca60f0fc9c

mkdir -p unpacked
tar -xzf mbx.tar.gz -C unpacked
test -f unpacked/mbx
for name in mbx mise; do
    destination="$HOME/.local/bin/$name"
    if [[ -e "$destination" ]]; then
        test -x "$destination"
        if [[ "$name" == mbx ]]; then
            test "$("$destination" --version)" = "mbx 1.28.0"
        else
            "$destination" --version | rg '^2026\.10\.20 '
        fi
    elif [[ "$name" == mbx ]]; then
        install -m 755 unpacked/mbx "$destination"
    else
        install -m 755 mise "$destination"
    fi
done

. "$HOME/.cargo/env"
export PATH="$HOME/.local/bin:$PATH"
unset RUSTUP_TOOLCHAIN CARGO_TARGET_DIR CARGO_BUILD_BUILD_DIR
mbx --version
mise --version
mbx setup --global
mbx setup --global --status
mise exec -- cargo --version
mise exec -- cargo +1.91.0 --version
