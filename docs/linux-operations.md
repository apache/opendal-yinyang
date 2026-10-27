# Linux operations

This guide qualifies one installed `yy` tree for Ubuntu 24.04 x86_64 with
systemd user services. It covers object-backed Managed Mount and finite Managed
Sync, plus the authenticated loopback metadata service for combinations that
set `publication: "service"`.

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
| Finite Managed Sync | Qualified by the object lifecycle smoke | Requires a service-volume Sync smoke |

Amazon S3 and MinIO-compatible storage are separate matrix entries even when
they use the same S3 provider code. Record the exact backend and endpoint in
qualification evidence; a MinIO run does not qualify Amazon S3 or another
non-loopback deployment.

Managed Mount and Managed Sync remain experimental. The metadata service is one
authenticated single-host loopback authority, not TLS, HA or a network-safe
service. There is no automatic Sync watcher or timer.

## Prerequisites

On the tested host:

```shell
sudo apt-get update
sudo apt-get install --yes ca-certificates fuse3
rustc --version
cargo --version
```

Build with Rust 1.91.0 or newer. The repository pins its development toolchain
in `rust-toolchain.toml`. A service-volume host also needs enough local space
for the SQLite authority and immutable storage content.

Do not put the environment file or state under a synchronized or mounted source
tree.

## Install a clean prefix

Run these commands from the repository checkout. The version is read from the
workspace, and the working directory is intentionally not the installation
prefix:

```shell
set -eu
umask 077

YY_VERSION="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name == "yinyang") | .version')"
YY_ROOT="$HOME/.local/lib/yinyang"
YY_RELEASE="$YY_ROOT/$YY_VERSION"
YY_STATE="$HOME/.local/state/yinyang"
YY_CONFIG="$HOME/.config/yinyang"
YY_UNITS="$HOME/.config/systemd/user"

cargo build --release --locked --bin yy
install -d -m 0700 "$YY_RELEASE/bin" "$YY_STATE" "$YY_STATE/metadata" \
  "$YY_CONFIG/volumes" "$YY_UNITS" "$HOME/YinYang"
install -m 0755 target/release/yy "$YY_RELEASE/bin/yy"
ln -sfn "$YY_VERSION" "$YY_ROOT/current"

install -m 0600 deploy/linux/environment.example "$YY_CONFIG/environment"
install -m 0644 deploy/linux/yinyang-serve.service "$YY_UNITS/"
install -m 0644 deploy/linux/yinyang-mount@.service "$YY_UNITS/"
install -m 0644 deploy/linux/yinyang-sync@.service "$YY_UNITS/"
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

Record `git rev-parse HEAD`, the `sha256sum` output, `YY_VERSION` and the host
identity with the qualification evidence. The unit files are user services:
installing them does not enable a system service, change the kernel FUSE
configuration, or start anything at boot. Do not run `loginctl enable-linger`
as part of this workflow.

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
information, not as a CLI runtime restriction. The `scope` object states the
boundary: the state probe writes and removes one namespaced temporary file,
does not initialize the catalog or acquire a runtime lease, and the authority
check only observes the latest revision.

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

## Reinstall and uninstall

Until issue #79 defines a supported version migration contract, only an
experimental same-version reinstall/rollback is in scope. Stop all units,
preserve `$HOME/.local/state/yinyang` and every volume document, install
another build of the exact same declared version into its versioned directory,
switch `current`, then start and rerun `doctor`. Do not infer cross-version
compatibility from this procedure.

Uninstall the executable and user units while keeping state and configuration:

```shell
systemctl --user disable --now yinyang-mount@object.service || true
systemctl --user disable --now yinyang-serve.service || true
rm -f "$HOME/.config/systemd/user/yinyang-serve.service" \
  "$HOME/.config/systemd/user/yinyang-mount@.service" \
  "$HOME/.config/systemd/user/yinyang-sync@.service"
systemctl --user daemon-reload

rm -rf "$HOME/.local/lib/yinyang/current"
rm -rf "$HOME/.local/lib/yinyang/$YY_VERSION"
```

Do not remove `$HOME/.config/yinyang` or
`$HOME/.local/state/yinyang` during uninstall unless the operator has made a
separate explicit retention decision. The same applies to
`$HOME/YinYang/<volume>`: it may contain acknowledged local bytes or a Sync
source, not a disposable install artifact.
