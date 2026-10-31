# Linux trial distribution

Status: experimental Ubuntu 24.04 x86_64 trial artifact. The workflow produces a
directly runnable `yy` binary for a first trial and a local qualification archive
for maintainer installation and compatibility checks. Neither artifact is an
official Apache Software Foundation release, tag or signature.

## Download and run yy

Open the
[Linux trial distribution workflow](https://github.com/apache/opendal-yinyang/actions/workflows/linux-trial.yml).
Choose a successful run and download its `yinyang-linux-x86_64` artifact.
GitHub supplies Actions artifacts as ZIP files and requires a signed-in GitHub
account for browser downloads. The artifact expires after 14 days and is not a
stable release URL.

Extract and verify the binary before running it:

```shell
mkdir -p yinyang-linux-x86_64
unzip yinyang-linux-x86_64.zip -d yinyang-linux-x86_64
cd yinyang-linux-x86_64

sha256sum --check yy.sha256
chmod +x yy
./yy --help
```

Actions artifact ZIP extraction does not preserve the executable mode, so
`chmod +x yy` is required. The checksum detects a different `yy` from the
sidecar in the same artifact; it is not a signature.

The artifact contains:

- `yy`;
- `yy.sha256`;
- `LICENSE` and `NOTICE`;
- `BUILD-MANIFEST`, which records the full source commit, target and
  `yy_sha256`.

Install `ca-certificates` and `fuse3` before using Managed Mount. Storage
credentials and an S3-compatible endpoint remain operator-supplied:

```shell
sudo apt-get update
sudo apt-get install --yes ca-certificates fuse3
```

`doctor` requires an existing strict volume document. It does not create or
initialize one:

```shell
./yy --volume /absolute/path/to/volume.json doctor --json
```

See [Linux operations](linux-operations.md) for volume configuration, state
locations and user-service setup. The direct binary does not require Cargo,
Rust, Git, Python or an installer.

## Supported trial matrix

The artifact target is one named combination:

- Ubuntu 24.04 x86_64;
- the local `yy` executable built from the recorded source commit;
- Managed Mount, finite Managed Sync, operational diagnostics and recovery as
  qualified in the existing Linux guides;
- object publication and authenticated single-host loopback service publication;
- isolated MinIO-compatible Silo for deterministic upgrade and recovery
  fixtures.

Amazon S3 evidence remains a separate backend record. A Silo run does not
qualify another endpoint, provider, host, platform, multi-host authority or
production capacity. Direct modes remain part of the broader Linux matrix, but
this trial does not add a Direct-specific reclamation or migration promise.

## Maintainer archive and installation

The workflow also packages the same binary into an inspectable archive. This
path remains the maintainer entry point for immutable-prefix installation,
retained-state recovery and exact cross-version qualification. It is not
required for a first trial of `yy`.

The archive name and release directory use both the Cargo package version and a
12-character source suffix. Two binaries with the same package version
therefore have different release directory names.

### Build the archive

The build must run from a clean Ubuntu 24.04 x86_64 checkout:

```shell
mkdir -p /var/tmp/yinyang-artifacts
tools/linux/package-release.sh --output-dir /var/tmp/yinyang-artifacts
```

The output directory receives:

- `yy`, `yy.sha256`, `LICENSE`, `NOTICE` and `BUILD-MANIFEST`;
- `yinyang-<version>-ubuntu-24.04-x86_64-<source>.tar.gz`;
- the adjacent `.tar.gz.sha256` sidecar.

The archive includes `BUILD-MANIFEST`, `PREREQUISITES`, `INSTALL`, `LICENSE`,
`NOTICE`, `bin/yy`, the three systemd user-unit templates, the environment
example, operational documents and the packaged verifier and installer.

The build script refuses a dirty tree or existing output files and normalizes
archive path ordering, ownership and modification time. It does not claim
bit-for-bit reproducible compiler output.

### Verify the archive

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

### Install and activate

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

### Upgrade and downgrade boundary

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

The actual installed-target stage-3 and bounded Amazon S3 record is
[`linux-stage3-trial-results.md`](linux-stage3-trial-results.md). It records
the exact archive, installed binary and later script-only qualification source
separately.

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
x86_64 runner. It verifies the directly downloadable `yy` checksum, manifest
source identity and `--help`, then uploads the binary, checksum, LICENSE, NOTICE
and manifest as the `yinyang-linux-x86_64` artifact only after the other trial
checks succeed.

The workflow separately verifies the archive using its packaged scripts,
installs the actual archive into a versioned prefix, exercises checksum
rejection, immutable installation, explicit quiesced activation and installed
diagnostics, and runs recovery.

The same workflow installs the pinned `fd1338c` producer and runs
`linux_prefix_upgrade` with the archive's installed target binary. It then runs
the immutable stage-3 fixture upgrade with that installed binary. It uploads
the archive, checksum, manifest, exact binary hash, verifier output, upgrade
JSON and separate stage-3 JSON as ordinary short-lived Actions evidence. The
upload is not a release publication.

Hosted CI does not manufacture a systemd user session. It verifies the packaged
unit inventory; the actual user-manager lifecycle remains an explicit
qualified-host run described in the [qualification guide](linux-qualification.md).
