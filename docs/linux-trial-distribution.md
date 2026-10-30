# Linux trial distribution

Status: experimental Ubuntu 24.04 x86_64 trial artifact. This workflow produces
an inspectable local qualification archive, installs that exact archive into a
versioned user prefix, and exercises supported stopped-process transitions. It
does not create an official Apache Software Foundation release, tag, signature
or externally published artifact.

## Supported trial matrix

The archive target is one named combination:

- Ubuntu 24.04 x86_64;
- the local `yy` executable built from the recorded source commit;
- Managed Mount, finite Managed Sync, operational diagnostics and recovery as
  qualified in the existing Linux guides;
- object publication and authenticated single-host loopback service publication;
- isolated MinIO-compatible Silo for deterministic upgrade/recovery fixtures.

Amazon S3 evidence remains a separate backend record. A Silo run does not
qualify another endpoint, provider, host, platform, multi-host authority or
production capacity. Direct modes remain part of the broader Linux matrix, but
this trial installer does not add a Direct-specific reclamation or migration
promise.

The archive name and release directory use both the Cargo package version and a
12-character source suffix. Two binaries with the same package version therefore
have different release directory names.

## Prerequisites

Install the packages recorded in the archive's `PREREQUISITES` file. The tested
base needs:

```shell
sudo apt-get update
sudo apt-get install --yes ca-certificates fuse3
```

`sqlite3` is additionally needed by explicit backup and recovery acceptance.
A systemd user manager is only needed for the supplied user units. The consumer
does not need Cargo, Rust, Git, Python or an agent-host build wrapper.

Storage credentials and the service token remain operator-supplied. Keep the
environment file and all configuration and state outside the immutable release
prefix.

## Build the archive

The build must run from a clean Ubuntu 24.04 x86_64 checkout:

```shell
mkdir -p /var/tmp/yinyang-artifacts
tools/linux/package-release.sh --output-dir /var/tmp/yinyang-artifacts
```

The output directory receives:

- `yinyang-<version>-ubuntu-24.04-x86_64-<source>.tar.gz`;
- the adjacent `.tar.gz.sha256` sidecar.

The archive includes `BUILD-MANIFEST`, `PREREQUISITES`, `INSTALL`, `LICENSE`,
`NOTICE`, `bin/yy`, the three systemd user-unit templates, the environment
example, operational documents and the packaged verifier and installer.

The build script refuses a dirty tree and normalizes archive path ordering,
ownership and modification time. It does not claim bit-for-bit reproducible
compiler output.

## Verify the archive

Verification is independent of installation:

```shell
tools/linux/verify-release.sh \
  --archive /var/tmp/yinyang-artifacts/yinyang-*.tar.gz \
  --source-commit "$(git rev-parse HEAD)"
```

The verifier checks the adjacent SHA-256 sidecar, path safety, duplication,
entry types, the exact archive inventory, manifest fields, release naming,
source identity, packaged binary SHA-256, LICENSE/NOTICE, installation
prerequisites and execution of `bin/yy --help`.

The checksum sidecar proves only that the archive matches the sidecar supplied
with it. It is not a signature and does not establish artifact authenticity or
an ASF release decision.

## Install and activate

Extract the archive outside the installation prefix and use the packaged
installer:

```shell
set -eu
umask 077

archive=/var/tmp/yinyang-artifacts/yinyang-<version>-ubuntu-24.04-x86_64-<source>.tar.gz
extract="$(mktemp -d)"
tar -xzf "$archive" -C "$extract"

installer="$extract/$(basename "$archive" .tar.gz)/share/yinyang/install-release.sh"
prefix="$HOME/.local/lib/yinyang"

"$installer" install --archive "$archive" --prefix "$prefix"
```

`install` first repeats archive verification. It then creates exactly one new
immutable `$prefix/<version>+<source>` directory and refuses to overwrite an
existing release. It does not touch configuration, staging, Sync state, service
state or the current symlink unless `--activate` is explicitly requested.

Stop every `yy` writer before committing the current selection:

```shell
"$installer" activate \
  --prefix "$prefix" \
  --release "<version>+<source>" \
  --quiesced
```

The `--quiesced` flag is an explicit operator assertion, not a process watcher.
The installer never starts, stops or restarts systemd units. Activation verifies
the new release and atomically updates `current`. It does not retain a pending
operation, state marker or automatic rollback action.

Install the supplied unit templates and environment file as described in
[Linux operations](linux-operations.md). Start the existing units only after the
new `current` target, binary SHA-256, `yy doctor` result and retained volume
state have been verified.

## Upgrade and downgrade boundary

The only trial upgrade route is a stopped, same-host replacement from one
immutable release directory to the next. The verified target may reopen state
only after its compatibility profiles admit every persistent profile reached by
that state.

`linux_prefix_upgrade` currently qualifies the exact `fd1338c` producer against
the selected target archive binary. It exercises retained pending, frozen and
Unknown requests, original receipts, finite Sync convergence and a non-empty
service WAL.

`linux_stage3_upgrade` separately qualifies the immutable `546f419` stage-3
corpus against the same selected target archive. The corpus was produced by the
accepted `546f419` trial archive binary and contains a genuine sparse pending
handle, a request dropped before commit, a committed request whose response was
lost, and a service metadata database copied with its non-empty WAL. The target
must preserve sparse bytes, publish pending work, retry the original frozen and
Unknown CommitIds/digests, resolve the lost receipt identity, reopen the service
WAL and pass `doctor`. The producer binary and target binary hashes are recorded
separately; the source tree being equal to a later main commit does not rename
the producer.

The installer intentionally has no rollback or downgrade command. Once a newer
binary has opened or written configuration, staging, Sync or service state, do
not relaunch an older binary against that state. An older release directory
remaining on disk preserves audit and recovery inputs; it is not permission to
run that binary. Existing compatibility preflight, a quiesced backup or an
isolated restore define the supported recovery route. Arbitrary old/new
downgrade remains unsupported.

## Local space and non-destructive retention

The installer can report the local versioned-prefix footprint without changing
anything:

```shell
"$installer" inspect --prefix "$prefix"
```

The report includes `current`, each release directory's `du -sb` size and the
retained release count. Its accounting scope is
`installed-release-directories-only`.

The report deliberately leaves these entries unknown:

- object-store content, live and retained versions, delete markers and
  non-atomic or incomplete listings;
- staging, Sync databases, metadata databases, WAL and SHM files;
- unresolved pending, frozen, Unknown, conflict and failed work;
- external readers and mounts that may pin older revisions;
- backend-specific shared physical packs, server-side retention and
  replication;
- backups, archives, checksum sidecars and CI evidence.

A known local byte count does not make any backend version reclaimable. An age
or count threshold is not deletion authorization.

This milestone performs no physical deletion, garbage collection, expiry or
automatic cleanup. Keep existing release directories, trial archives, checksum
sidecars, manifests and complete state until an operator has established reader
and unresolved-work quiescence, preserved the required backup, and reconciled
backend versions with an exact supported backend contract.

## Fresh-runner acceptance

The `.github/workflows/linux-trial.yml` workflow builds on a fresh Ubuntu 24.04
x86_64 runner, verifies the archive using its packaged scripts, installs the
actual archive into a versioned prefix, exercises checksum rejection,
immutable installation, explicit quiesced activation and installed diagnostics,
and runs recovery.

The same workflow installs the pinned `fd1338c` producer and runs
`linux_prefix_upgrade` with the archive's installed target binary. It then runs
the immutable stage-3 fixture upgrade with that installed binary. It uploads
the archive, checksum, manifest, exact binary hash, verifier output, upgrade
JSON and separate stage-3 JSON as ordinary short-lived Actions evidence. The
upload is not a release publication.

Hosted CI does not manufacture a systemd user session. It verifies the packaged
unit inventory; the actual user-manager lifecycle remains an explicit
qualified-host run described in the [qualification guide](linux-qualification.md).
