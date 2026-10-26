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

# Task-specific Ubuntu development host setup, not a product installer.
set -euo pipefail
umask 077
state_dir="$HOME/.local/state/yinyang-devbox"
mkdir -p "$state_dir"
exec > >(tee -a "$state_dir/setup.log") 2>&1
finish() {
    result=$?
    printf '%s\n' "$result" > "$state_dir/setup.exit.tmp"
    mv "$state_dir/setup.exit.tmp" "$state_dir/setup.exit"
}
trap finish EXIT
date -Is
git rev-parse HEAD
. /etc/os-release
test "$ID" = ubuntu
test "$VERSION_ID" = 24.04

sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential clang lld pkg-config libssl-dev libfuse3-dev fuse3 \
    git gh curl ca-certificates jq tmux ripgrep fd-find zsh tree \
    python3 nodejs npm unzip zip gdb strace sqlite3 docker.io
sudo systemctl enable --now docker
sudo modprobe fuse
test -c /dev/fuse
test -r /dev/fuse
test -w /dev/fuse
mkdir -p "$HOME/.local/bin"
ln -sf /usr/bin/fdfind "$HOME/.local/bin/fd"
# /usr/local/bin is present in both bash and zsh login shells.
sudo ln -sf /usr/bin/fdfind /usr/local/bin/fd
sudo hostnamectl set-hostname lance-fuse-demo

if ! test -x "$HOME/.cargo/bin/rustup"; then
    rustup_dir=$(mktemp -d)
    rustup_binary="$rustup_dir/rustup-init"
    curl --fail --location --retry 3 \
        https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init \
        --output "$rustup_binary"
    chmod 700 "$rustup_binary"
    "$rustup_binary" -y --profile minimal --default-toolchain stable
fi
. "$HOME/.cargo/env"
rustup component add clippy rustfmt rust-analyzer
rustup toolchain install 1.91.0 --profile minimal

binstall_dir=$(mktemp -d)
curl --fail --location --retry 3 \
    https://github.com/cargo-bins/cargo-binstall/releases/download/v1.26.1/cargo-binstall-x86_64-unknown-linux-musl.tgz \
    --output "$binstall_dir/binstall.tgz"
printf '%s  %s\n' 43d379fef8cb450961a14ff113e7bce7b5e1e52db709c776d186f928d8a20a40 \
    "$binstall_dir/binstall.tgz" | sha256sum --check
tar -xzf "$binstall_dir/binstall.tgz" -C "$binstall_dir"
install -m 755 "$binstall_dir/cargo-binstall" "$HOME/.cargo/bin/cargo-binstall"
cargo binstall --no-confirm --disable-telemetry \
    taplo-cli typos-cli hawkeye cargo-deny@0.14.22
sudo npm install --global @openai/codex
# No GitHub token, signing key or Codex login is copied to the host.
git config --global user.name Xuanwo
git config --global user.email github@xuanwo.io
git config --global fetch.prune true
git config --global push.autoSetupRemote true
git config --global commit.gpgsign false
git config --global tag.gpgsign false

image='docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z@sha256:635197cb9f36d01bee221d34d1c7d7960f6a95c48b0b6c01d99cd13bdae51a46'
if ! sudo docker container inspect yinyang-silo >/dev/null 2>&1; then
    sudo docker run -d --name yinyang-silo --restart unless-stopped \
        -p 127.0.0.1:9000:9000 \
        --mount type=volume,source=yinyang-silo-data,target=/data \
        -e MINIO_ROOT_USER=yinyang-test -e MINIO_ROOT_PASSWORD=yinyang-test-only \
        "$image" server /data
else
    sudo docker start yinyang-silo
fi
for attempt in {1..30}; do
    if curl --fail --silent http://127.0.0.1:9000/minio/health/ready; then
        break
    fi
    sleep 1
done
curl --fail http://127.0.0.1:9000/minio/health/ready
sudo docker exec yinyang-silo mcli alias set local http://127.0.0.1:9000 yinyang-test yinyang-test-only
sudo docker exec yinyang-silo mcli mb --ignore-existing local/yinyang-test
rustc --version
cargo --version
codex --version
fusermount3 --version
pkg-config --modversion fuse3
date -Is

