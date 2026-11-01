# Linux operations

This guide qualifies one installed `yy` tree for Ubuntu 24.04 x86_64 with
systemd user services. It covers object-backed Managed Mount and finite Managed
Sync, plus the authenticated loopback metadata service for combinations that
set `publication: "service"`.

For a first trial, download the standalone `yy` binary described in the
[trial distribution guide](linux-trial-distribution.md). This page covers the
maintainer installation, configuration and user-service lifecycle around that
binary.

The supported installation layout is deliberately small:

- `$HOME/.local/lib/yinyang/<version>` contains one immutable binary release;
- `$HOME/.local/lib/yinyang/current` points to the release used by the units;
- `$HOME/.config/yinyang` contains the environment file and volume documents;
- `$HOME/.local/state/yinyang` contains metadata authority, Mount and Sync state;
- `$HOME/YinYang/<volume>` is an ordinary directory used as a mountpoint or Sync
  source when following the examples.

Configuration, synchronized data, mount staging and metadata databases are
outside the installation prefix. Stopping a unit or removing the installation
does not delete them.

## Qualification matrix

The workflow records the exact host, binary and storage service in its evidence.
It does not turn "Ubuntu 24.04 x86_64" into a runtime restriction: `yy doctor`
reports whether the current host is the tested combination, while the CLI can
still be used on other Linux hosts without claiming they are qualified.

| Frontend | Object publication | Loopback service publication |
| --- | --- | --- |
| Managed Mount | Qualified by the object lifecycle smoke | Qualified by the service lifecycle smoke |
| Finite Managed Sync | Qualified by the object lifecycle smoke | Qualified by the service lifecycle smoke |

Amazon S3 and MinIO-compatible storage are separate matrix entries even when
they use the same S3 provider code. Record the exact backend and endpoint in
qualification evidence; a MinIO run does not qualify Amazon S3 or another
non-loopback deployment.

| Storage backend | Operational evidence |
| --- | --- |
| Amazon S3, `us-east-2`, provider endpoint | Passed at source `7759518`: `ops-real-s3-7759518-role-r1`, exit 0, one test passed in 7.97s on 2026-10-28 |
| Isolated MinIO-compatible Silo, loopback endpoint | Exact-CLI and systemd user-unit lifecycle passed at source `7759518`: `ops-cli-7759518` and `ops-systemd-7759518` |

Current Silo qualification covers both the direct exact-CLI lifecycle and the
actual `systemctl --user` unit lifecycle, including object and
authenticated-loopback-service Mount/Sync, mounted write, fsync, read, restart
and finite Sync convergence. The real Amazon S3 run used the installed binary
prefix with no endpoint override. Evidence is limited to the named backend
class and host; it is not a capacity, multi-host or HA claim.

The installed-prefix upgrade harness passed at final integrated source
`17361970d1722ea3d60c5381bbb5323d11818184` against the exact `fd1338c`
producer. It retained frozen and pending handles, the original file and Sync
receipts, and a 74,192-byte service WAL before target recovery. The target
archive binary SHA-256 is
`a6724f23fd5b5120f14b17dd9290db2cd337ebe8f934758accc953317f3c154c`.
The earlier `79506c0` run remains preparatory history, not a substitute for
this source-specific result.

The tested `target/debug/yy` from source `7759518` had SHA-256
`b9acff232c66f6dd898deeccb315e2a1f70af4e9e37959793bc7ba98425ed705`.

Managed Mount and Managed Sync remain experimental. The metadata service is one
authenticated single-host loopback authority, not TLS, HA or a network-safe
service. There is no automatic Sync watcher or timer.

Read-only Direct Mount is implemented as a separate experimental frontend. It
does not use staging, the metadata service or Managed recovery state. Writable
Direct, when enabled by its own accepted integration, requires a private
staging directory and uses Direct runtime state for pending, conflict and
Unknown work. Managed qualification below does not qualify either Direct mode.

## Prerequisites

On the tested host:

```shell
sudo apt-get update
sudo apt-get install --yes ca-certificates fuse3 jq python3
rustc --version
cargo --version
```

Build with Rust 1.91.0 or newer. The repository pins its development toolchain
in `rust-toolchain.toml`. A service-volume host also needs enough local space
for the SQLite authority and immutable storage content.

Do not put the environment file or state under a synchronized or mounted source
tree.

## Build a local qualification artifact

The repository can build a traceable local archive without signing, tagging or
publishing it:

```shell
mkdir -p /var/tmp/yinyang-artifacts
tools/linux/package-release.sh --output-dir /var/tmp/yinyang-artifacts
tools/linux/verify-release.sh \
  --archive /var/tmp/yinyang-artifacts/yinyang-*.tar.gz \
  --source-commit "$(git rev-parse HEAD)"
```

The archive contains `bin/yy`, the three systemd user-unit templates, the
environment example, operational documentation, `LICENSE`, `NOTICE`,
`PREREQUISITES`, `BUILD-MANIFEST`, `INSTALL` and the packaged verifier and
installer. The manifest records the package version, release identity, source
commit, source date, target, Cargo/Rust versions and binary SHA-256. The script
refuses a dirty worktree and requires Linux x86_64 so the archive cannot silently
claim a source/build identity it did not test.

The same output directory also contains the directly runnable `yy`,
`yy.sha256`, `LICENSE`, `NOTICE` and `BUILD-MANIFEST`. The workflow uploads
those files separately as the consumer trial artifact; the archive remains the
input for immutable-prefix installation and compatibility qualification.

The script requires Ubuntu 24.04 x86_64, the tested host combination. This
artifact is local qualification input, not an Apache Software Foundation
release. It has no ASF signature, vote, provenance approval or publication
authorization. See the [trial distribution guide](linux-trial-distribution.md)
for the exact inventory, checksum and retention boundaries.

## Install a clean prefix

Extract the verified archive outside the installation prefix and use its packaged
installer. The working directory is intentionally not the installation prefix:

```shell
set -eu
umask 077

YY_ARCHIVE=/var/tmp/yinyang-artifacts/yinyang-<version>-ubuntu-24.04-x86_64-<source>.tar.gz
YY_ROOT="$HOME/.local/lib/yinyang"
YY_STATE="$HOME/.local/state/yinyang"
YY_CONFIG="$HOME/.config/yinyang"
YY_UNITS="$HOME/.config/systemd/user"

install -d -m 0700 "$YY_STATE" "$YY_STATE/metadata" \
  "$YY_CONFIG/volumes" "$YY_UNITS" "$HOME/YinYang"

YY_EXTRACT="$(mktemp -d)"
tar -xzf "$YY_ARCHIVE" -C "$YY_EXTRACT"
YY_ARCHIVE_ROOT="$(basename "$YY_ARCHIVE" .tar.gz)"
YY_INSTALLER="$YY_EXTRACT/$YY_ARCHIVE_ROOT/share/yinyang/install-release.sh"

YY_INSTALL_OUTPUT="$("$YY_INSTALLER" install \
  --archive "$YY_ARCHIVE" \
  --prefix "$YY_ROOT" \
  --activate \
  --quiesced)"
printf '%s\n' "$YY_INSTALL_OUTPUT"
YY_RELEASE="$(awk -F= '$1 == "activated_release" { print $2 }' <<<"$YY_INSTALL_OUTPUT")"

install -m 0600 "$YY_ROOT/current/share/yinyang/environment.example" "$YY_CONFIG/environment"
install -m 0644 "$YY_ROOT/current/lib/systemd/user/yinyang-serve.service" "$YY_UNITS/"
install -m 0644 "$YY_ROOT/current/lib/systemd/user/yinyang-mount@.service" "$YY_UNITS/"
install -m 0644 "$YY_ROOT/current/lib/systemd/user/yinyang-sync@.service" "$YY_UNITS/"
systemctl --user daemon-reload

"$YY_ROOT/current/bin/yy" --help
sha256sum "$YY_ROOT/current/bin/yy"
```

Replace every `replace-with-...` value in
`$HOME/.config/yinyang/environment` with the absolute paths for this
installation:

```text
YINYANG_BIN=/home/example/.local/lib/yinyang/current/bin/yy
YINYANG_CONFIG_DIR=/home/example/.config/yinyang/volumes
YINYANG_MOUNT_ROOT=/home/example/YinYang
YINYANG_METADATA_DB=/home/example/.local/state/yinyang/metadata/metadata.db
YINYANG_SERVICE_LISTEN=127.0.0.1:7447
```

The environment file must stay mode `0600`. The config and state parent
directories were created mode `0700` above; the mount unit uses `UMask=0022` so
the FUSE frontend's supported `0644`/`0755` files are not weakened to
unrepresentable `0600` modes. The service and Sync units use `UMask=0077`.

Record the archive SHA-256, `BUILD-MANIFEST`, `git rev-parse HEAD`, the
`sha256sum` output and the host identity with the qualification evidence. The
release directory also includes a 12-character source suffix because two
development binaries can share one package version. The unit files are user
services: installing them does not enable a system service, change the kernel
FUSE configuration, or start anything at boot. Do not run
`loginctl enable-linger` as part of this workflow.

## Configure storage and secrets

Edit `$HOME/.config/yinyang/environment` and keep it mode `0600`. It is consumed
only by the user units. Never pass `YINYANG_SERVICE_TOKEN` on a command line or
commit the file.

For Amazon S3, omit `YINYANG_S3_ENDPOINT`:

```text
YINYANG_STORAGE_PROFILE=amazon-s3
YINYANG_S3_BUCKET=example-bucket
YINYANG_S3_REGION=us-east-2
```

Use either explicit `YINYANG_S3_ACCESS_KEY_ID` and
`YINYANG_S3_SECRET_ACCESS_KEY` values, with
`YINYANG_S3_SESSION_TOKEN` for temporary credentials, or the ambient AWS
credential chain. The `YINYANG_S3_*` suffix is mapped to the OpenDAL `s3`
configuration, so the temporary token key is `SESSION_TOKEN`. For an isolated
MinIO-compatible service, also set:

```text
YINYANG_STORAGE_PROFILE=minio
YINYANG_S3_ENDPOINT=http://127.0.0.1:9000
```

Service publication also needs one random token of 32 to 1024 bytes shared by
the server and every client:

```text
YINYANG_SERVICE_TOKEN=<random-token-of-32-to-1024-bytes>
```

Generate the token with a local credential manager or `openssl rand -hex 32`.
The environment file must not be readable by other users. The loopback
transport is authenticated but not encrypted; it has one full-authority token
and no per-user authorization.

The `YINYANG_S3_ROOT` prefix must identify this installation's isolated
filesystem. Each unit reads `$HOME/.config/yinyang/environment-<instance>`
after the common environment file, so Mount, Sync and metadata-service
instances can use different roots without putting credentials in unit files.
Do not point two installations at the same prefix and expect independent
authority.

For the examples in this guide, create owner-only root overlays:

```shell
printf '%s\n' 'YINYANG_S3_ROOT=qualification/linux-host-01/object' \
  > "$HOME/.config/yinyang/environment-object"
printf '%s\n' 'YINYANG_S3_ROOT=qualification/linux-host-01/sync' \
  > "$HOME/.config/yinyang/environment-sync"
printf '%s\n' 'YINYANG_S3_ROOT=qualification/linux-host-01/service' \
  > "$HOME/.config/yinyang/environment-service"
chmod 0600 "$HOME/.config/yinyang/environment-"{object,sync,service}
```

## Create volume documents

The examples below place state under the dedicated state root, outside the
mountpoint and Sync source. Create the object-backed Mount document:

```shell
cat > "$HOME/.config/yinyang/volumes/object.json" <<EOF
{
  "name": "object",
  "model": "managed",
  "access": "mount",
  "frontend": "fuse",
  "publication": "object",
  "storage_profile": "amazon-s3",
  "staging": "$HOME/.local/state/yinyang/object",
  "read_only": false,
  "require": [],
  "service_address": null
}
EOF
chmod 0600 "$HOME/.config/yinyang/volumes/object.json"
mkdir -p "$HOME/YinYang/object"
```

For object-backed finite Sync, use a different staging directory and an
ordinary source directory:

```shell
cat > "$HOME/.config/yinyang/volumes/sync.json" <<EOF
{
  "name": "sync",
  "model": "managed",
  "access": "sync",
  "frontend": "library",
  "publication": "object",
  "storage_profile": "amazon-s3",
  "staging": "$HOME/.local/state/yinyang/sync",
  "read_only": false,
  "require": [],
  "service_address": null
}
EOF
chmod 0600 "$HOME/.config/yinyang/volumes/sync.json"
mkdir -p "$HOME/YinYang/sync"
```

The volume document's `storage_profile` selects the profile for `--volume`
commands. `YINYANG_STORAGE_PROFILE` remains the selection for commands that do
not take a volume, including `yy serve` and one-shot publication. They normally
have the same value, but a volume command does not require the global variable.
For MinIO, change the document to `minio` and keep the service environment's
global profile consistent when service publication is used. For service
publication, change `publication` to `service` and set the loopback
`service_address`, for example `"service_address": "127.0.0.1:7447"`.
`yy doctor` reports the volume's profile and does not print credential values.

Initialize the filesystem and the Sync binding before starting the units:

```shell
YY="$HOME/.local/lib/yinyang/current/bin/yy"
yyvol() {
  name="$1"
  shift
  systemd-run --user --wait --collect --pipe \
    --property="EnvironmentFile=$HOME/.config/yinyang/environment" \
    --property="EnvironmentFile=$HOME/.config/yinyang/environment-$name" \
    "$YY" "$@"
}

yyvol object --volume "$HOME/.config/yinyang/volumes/object.json" capabilities
yyvol object create
yyvol sync --volume "$HOME/.config/yinyang/volumes/sync.json" sync init \
  --directory "$HOME/YinYang/sync"
```

`capabilities` is an admission check. `sync init` creates or validates the
local binding but does not publish a reconciliation. The temporary
`systemd-run` process reads the same environment files and overlays as the
installed units, so credentials never enter command arguments.

## Start, inspect and stop

Start a loopback metadata service before any service-publication client:

```shell
systemctl --user start yinyang-serve.service
systemctl --user is-active yinyang-serve.service
journalctl --user -u yinyang-serve.service --since "10 minutes ago"
```

For the service-authority Mount and Sync examples, create separate volume
documents that point at the same loopback listener:

```shell
cat > "$HOME/.config/yinyang/volumes/service.json" <<EOF
{
  "name": "service",
  "model": "managed",
  "access": "mount",
  "frontend": "fuse",
  "publication": "service",
  "storage_profile": "amazon-s3",
  "staging": "$HOME/.local/state/yinyang/service",
  "read_only": false,
  "require": [],
  "service_address": "127.0.0.1:7447"
}
EOF

cat > "$HOME/.config/yinyang/volumes/service-sync.json" <<EOF
{
  "name": "service-sync",
  "model": "managed",
  "access": "sync",
  "frontend": "library",
  "publication": "service",
  "storage_profile": "amazon-s3",
  "staging": "$HOME/.local/state/yinyang/service-sync",
  "read_only": false,
  "require": [],
  "service_address": "127.0.0.1:7447"
}
EOF

chmod 0600 "$HOME/.config/yinyang/volumes/service.json" \
  "$HOME/.config/yinyang/volumes/service-sync.json"
mkdir -p "$HOME/YinYang/service"
yyvol service --volume "$HOME/.config/yinyang/volumes/service.json" doctor
yyvol service-sync --volume "$HOME/.config/yinyang/volumes/service-sync.json" \
  sync init --directory "$HOME/YinYang/service-sync"
systemctl --user start yinyang-mount@service.service
```

Start a Managed Mount user service and verify the kernel mount identity:

```shell
systemctl --user start yinyang-mount@object.service
systemctl --user is-active yinyang-mount@object.service
findmnt --target "$HOME/YinYang/object"
```

The mount is ready only when `findmnt` reports `fuse.yinyang`.

Run one finite Sync pass through the user unit:

```shell
systemctl --user start yinyang-sync@sync.service
systemctl --user is-active yinyang-sync@sync.service
```

`yinyang-sync@.service` is a oneshot unit; `is-active` reports `active (exited)`
after the pass completes. That state means the finite command returned
successfully. It does **not** mean the local directory and remote namespace
converged. Convergence is established only by the status document:

```shell
yyvol sync --volume "$HOME/.config/yinyang/volumes/sync.json" \
  sync status --json |
  jq '{binding, remote, summary}'
```

Continue only when `summary.in_sync` is `true` and the remote authority is
available. Pending, conflict, unknown and failed work remain explicit; inspect
the full JSON and resolve it before treating the run as complete.

Ask for a non-destructive readiness report at any time:

```shell
systemctl --user status yinyang-mount@object.service --no-pager
yyvol object --volume "$HOME/.config/yinyang/volumes/object.json" doctor --json
```

`yy doctor` exits `0` when all checks pass and `1` when any check requires
action. Its `tested_host` field records whether this is the Ubuntu 24.04
x86_64 qualification combination; an untested Linux host is reported as
information, not as a CLI runtime restriction. For Managed, the `scope` object
states that the state probe writes and removes one namespaced temporary file,
does not initialize the catalog or acquire a runtime lease, and the authority
check only observes the latest revision. Read-only Direct performs no state
probe and reports one recursive read-only namespace observation. Writable
Direct probes its configured staging path with one temporary owner-only file
and inspects `DirectRuntime` read-only for profile/schema/binding and retained
pending, conflict or Unknown state; it does not create `runtime.db`, run
recovery or take the runtime lease.

Stop services without deleting state:

```shell
systemctl --user stop yinyang-mount@object.service
systemctl --user stop yinyang-mount@service.service
systemctl --user stop yinyang-serve.service
systemctl --user stop yinyang-sync@sync.service
systemctl --user stop yinyang-sync@service-sync.service
```

Use `systemctl --user restart yinyang-mount@object.service` or
`systemctl --user restart yinyang-serve.service` when diagnosing a process
restart. Mount and service state remain under `$HOME/.local/state/yinyang`;
finite Sync state remains under its configured staging directory. Stopping a
unit is not a backup.

## Diagnose failures

Start with the command boundary rather than the process exit alone:

```shell
yyvol object --volume "$HOME/.config/yinyang/volumes/object.json" doctor --json
systemctl --user status yinyang-mount@object.service --no-pager
journalctl --user -u yinyang-mount@object.service --since "30 minutes ago"
```

| Symptom | What to check | Recovery |
| --- | --- | --- |
| `doctor` reports `environment` | `YINYANG_S3_BUCKET`, profile, endpoint and access-key pair presence | Correct the environment file; do not print values |
| `doctor` reports `authority` with a 403 | Credential expiry, bucket policy, and the exact S3 backend | Use `tested_host` and the authority error; do not guess whether the cause is a key or policy |
| `doctor` reports `namespace` with invalid names or a collision | The named object keys and the Direct mapping contract | Rename or remove the object key; Direct does not filter or rewrite it |
| `doctor` reports a connection or timeout | `service_address`, listener status, endpoint DNS/TLS and outbound network | Restart the loopback service or correct the endpoint; preserve state |
| `doctor` reports `local-state` | State parent permissions, filesystem space and path overlap with mounted/source data | Fix the path or free space; do not delete staging or WAL files |
| `doctor` reports `mount-state` pending/frozen/conflict | The volume `status` output and journal | Preserve staging, inspect the handle/ledger, and resolve the exact uncertain request |
| `mount` exits within the 5-second teardown grace | Busy descriptors or a pending refresh | Stop the client, inspect the mount, and retry; do not report bytes as published |
| `sync status` has `in_sync: false` | `pending`, `unknown`, `conflicts`, `failed`, remote availability | Resolve or export the named work; successful `sync run` is not convergence |

A process death is recovered from durable state on the next start. Do not
replace a live SQLite database with the main file alone: WAL and durable
uncertain requests are part of the state. Backup and restore procedures are
separate operational work; this guide only guarantees that unit stop and
binary-prefix removal preserve state.

## Installed upgrade

The only supported installed transition is a stopped, same-host replacement for
an exact source pair recorded by acceptance. The producer is
`fd1338c2f924a90e9da752b9158e748d1d3acaf4`; the target is the final accepted
candidate that passes `tests/linux_prefix_upgrade.rs`. A build from another
source does not inherit this result.

Before switching:

1. Stop every `yinyang-serve.service`,
   `yinyang-mount@<volume>.service` and completed
   `yinyang-sync@<volume>.service`.
2. Confirm no `yy` process, FUSE mount or writer remains for the installation.
3. Preserve every volume document, Mount staging directory, Sync state and
   metadata database with its `-wal` and `-shm` files. Do not copy only SQLite
   main files.
4. Keep the old release directory for audit, but do not point `current` back to
   it after the new binary has opened or written state.

Install the new binary into its own directory and atomically replace the
symlink:

```shell
set -eu
umask 077
YY_ROOT="$HOME/.local/lib/yinyang"
YY_RELEASE="0.0.24+replace-with-target-source-prefix"
YY_DIR="$YY_ROOT/$YY_RELEASE"

install -d -m 0700 "$YY_DIR/bin"
install -m 0755 /path/to/new/yy "$YY_DIR/bin/yy"
sha256sum "$YY_DIR/bin/yy"
ln -s "$YY_RELEASE" "$YY_ROOT/.current.new"
mv -Tf "$YY_ROOT/.current.new" "$YY_ROOT/current"
systemctl --user daemon-reload
```

Start the retained units and verify the transition before resuming clients:

```shell
systemctl --user start yinyang-serve.service
systemctl --user start yinyang-mount@object.service
findmnt --target "$HOME/YinYang/object"
yyvol object --volume "$HOME/.config/yinyang/volumes/object.json" doctor --json
yyvol object --volume "$HOME/.config/yinyang/volumes/object.json" status
yyvol sync --volume "$HOME/.config/yinyang/volumes/sync.json" sync status --json
```

Accept the upgrade only when retained pending/Unknown handles and original
receipts are unchanged, locally acknowledged bytes are readable, the metadata
service opens the same authority, `PRAGMA integrity_check` reports `ok`, and a
finite Sync reaches `summary.in_sync=true`. The installed-prefix acceptance uses
the exact repository unit templates and versioned `current` symlink, but runs
the state transition as stopped processes so it is independent of a particular
user session. The existing systemd lifecycle test separately covers start,
restart and stop through `systemctl --user`.

Arbitrary downgrade is not supported. If the target binary has opened or written
newer local state, atomically switching `current` back is an unsupported
operation even when the older directory remains. Use an isolated restore of a
backup taken before the upgrade when recovery is required.

## Uninstall

Uninstall the executable and user units while keeping state and configuration:

```shell
systemctl --user disable --now yinyang-mount@object.service || true
systemctl --user disable --now yinyang-serve.service || true
rm -f "$HOME/.config/systemd/user/yinyang-serve.service" \
  "$HOME/.config/systemd/user/yinyang-mount@.service" \
  "$HOME/.config/systemd/user/yinyang-sync@.service"
systemctl --user daemon-reload

rm -f "$HOME/.local/lib/yinyang/current"
rm -rf "$HOME/.local/lib/yinyang/$YY_RELEASE"
```

Do not remove `$HOME/.config/yinyang` or
`$HOME/.local/state/yinyang` during uninstall unless the operator has made a
separate explicit retention decision. The same applies to
`$HOME/YinYang/<volume>`: it may contain acknowledged local bytes or a Sync
source, not a disposable install artifact.

Bounded sustained operation, sampled RSS/state/current-key growth and backend
retained-version inventory when exposed, plus the explicit non-capacity limits,
are recorded in
[`linux-operating-envelope.md`](linux-operating-envelope.md).
