# Apache OpenDAL™ YinYang

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![MSRV 1.91.0]][msrv] [![Apache 2.0 licensed]][license] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal-yinyang/ci.yml?branch=main
[actions]: https://github.com/apache/opendal-yinyang/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/yinyang.svg
[crates.io]: https://crates.io/crates/yinyang
[crate downloads]: https://img.shields.io/crates/d/yinyang.svg
[msrv 1.91.0]: https://img.shields.io/badge/MSRV-1.91.0-green?logo=rust
[msrv]: https://www.whatrustisit.com
[apache 2.0 licensed]: https://img.shields.io/crates/l/yinyang
[license]: https://www.apache.org/licenses/LICENSE-2.0
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

Apache OpenDAL™ YinYang is a cross-platform filesystem foundation.

> [!IMPORTANT]
> **Status: active redesign**
>
> We are actively working on the design of the next Apache OpenDAL™ YinYang release under
> [RFC-0016]. The core provides experimental indexed object-storage transactions,
> retained snapshots, and authenticated range-verifiable content. A durable
> metadata service and recoverable Managed file handles share the transaction
> contract. The `yy` CLI publishes and restores directories, operates configured
> Managed and Direct volumes, and provides Linux FUSE Managed and Direct Mount.

## Current support

- **Managed Mount:** The experimental Linux FUSE frontend supports read-only and
  writable configurations through `yy --volume CONFIG mount MOUNTPOINT`. The
  native acceptance suite (`cargo x test-linux-fuse`) runs every real-mount
  scenario in GitHub Linux CI against both publication authorities, covering
  saves, failure retention, refresh, crash recovery, read-only range
  hydration, incremental writes, retention and teardown. See the
  [frontend contract](specs/linux-fuse.md) and
  [acceptance guide](docs/linux-fuse-acceptance.md). This is experimental
  native validation, not release support.
- **Managed Sync:** The Linux ordinary-directory frontend reconciles a directory
  with a Managed volume through `yy --volume CONFIG sync init|run|status|export|resolve`
  and a versioned `yinyang.sync.status.v1` document. See the
  [Linux Managed Sync contract](specs/linux-sync.md) and the
  [Sync runtime](specs/sync-runtime.md). It stays experimental, runs only on
  Linux, and installs no automatic watcher or daemon.
- **macOS prototypes:** The experimental FSKit and File Provider frontends are
  isolated macOS 27 / Xcode 27 prototypes. They share the native bridge and
  runtimes, but ordinary volume CLI admission does not select them. See the
  [macOS frontend contract](specs/macos-frontends.md).
- **Direct:** The library and Linux FUSE frontend browse, read and
  conditionally write an existing S3-compatible object namespace without
  importing it into Managed metadata. Keys use a reversible Linux-name mapping,
  recursive listing is an explicit SIGHUP-refreshed observation, and each
  descriptor pins a real object version or uses conditional ETag ranges.
  Writable Direct Mount publishes whole objects conditionally with retained
  local generations and safe-save rename outcomes, and Direct Sync reconciles
  an ordinary Linux directory with the namespace. See the
  [Direct contract](specs/direct.md),
  [mutation runtime](specs/direct-mutations.md) and
  [Direct Sync contract](specs/direct-sync.md). The current MinIO-compatible
  Silo qualification does not cover a namespace whose LIST hides a descendant
  behind an existing file-prefix alias; the selected service must enumerate
  every current key.

Every change follows the [design principles](specs/design-principles.md).

The [implementation roadmap](https://github.com/apache/opendal-yinyang/issues/67)
tracks the remaining integration and platform work.
The closed historical umbrella
[PR #26](https://github.com/apache/opendal-yinyang/pull/26) explicitly was not
intended to merge and is not the active implementation stack.

## Try the Linux binary

The [Linux trial distribution workflow](https://github.com/apache/opendal-yinyang/actions/workflows/linux-trial.yml)
publishes an experimental Ubuntu 24.04 x86_64 `yy` binary as a short-lived
GitHub Actions artifact. Choose a successful run and download
`yinyang-linux-x86_64`:

```shell
mkdir -p yinyang-linux-x86_64
unzip yinyang-linux-x86_64.zip -d yinyang-linux-x86_64
cd yinyang-linux-x86_64

sha256sum --check yy.sha256
chmod +x yy
./yy --help
./yy --volume /absolute/path/to/volume.json doctor --json
```

GitHub requires a signed-in account for browser artifact downloads and expires
the artifact after 14 days. See the
[Linux trial distribution guide](docs/linux-trial-distribution.md) for the
artifact contents, supported matrix and volume setup.

## Publish and restore a directory

Build the experimental CLI with `cargo build --bin yy`. It currently connects
to Amazon S3 or MinIO. Create a bucket first, then configure OpenDAL through
`YINYANG_S3_*` environment variables:

```shell
export YINYANG_S3_BUCKET=my-bucket
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ROOT=my-filesystem
# For MinIO, also set YINYANG_S3_ENDPOINT and YINYANG_STORAGE_PROFILE=minio.
# Supply credentials using YINYANG_S3_ACCESS_KEY_ID and
# YINYANG_S3_SECRET_ACCESS_KEY, or the OpenDAL AWS credential chain.

target/debug/yy create
target/debug/yy publish ./source
target/debug/yy status
target/debug/yy restore ./new-destination
```

Keep the source directory unchanged during publication. Publishing a later
complete snapshot requires `publish ./source --replace`: remote-only paths are
removed. Publication prints a commit UUID; retain it and query `yy receipt UUID`
if the result is uncertain. A missing receipt does not prove failure while an
attempt may still complete. `--commit-id` selects an identity for a new request,
not permission to replan an old one. Add `--state DIR` to retain the frozen
request for restart-safe recovery:

```shell
target/debug/yy publish ./source --state ./publication-state --commit-id UUID
target/debug/yy publication inspect UUID --state ./publication-state --json
target/debug/yy publication retry UUID --state ./publication-state
target/debug/yy publication export UUID --state ./publication-state --destination ./original
```

`--state DIR` must be outside `SOURCE`; the journal directory cannot be the
source or one of its descendants. Keep the journal database and WAL files
together and outside mounted or synchronized source trees.

`publication inspect` reads only local state and needs no storage credentials.
`retry` and `export` never scan `SOURCE`; they use the original frozen request
and prepared manifest. Retrying a known identity with `publish` is rejected
instead of silently applying changed source bytes under the old commit identity.
Unknown outcomes exit with code 3 and do not establish failure. Conflict and
Retryable outcomes exit with code 4. Concurrent changes within the replaced
scope return a conflict without silently erasing them.

Restore requires a destination that does not exist. It verifies and installs
files individually without overwriting anything. An interrupted restore can
leave a partial directory; inspect it and restore into a new destination to
retry. `restore DESTINATION --revision REVISION` selects a retained historical
revision; without the option it restores latest. This is one-shot transfer, not
a background or bidirectional Sync engine.
See the [directory transfer contract](specs/directory-transfer.md) for supported
metadata, failure semantics, and limitations.

## Metadata service and recoverable files

The [metadata service](specs/metadata-service.md) provides an experimental local
SQLite authority with authenticated loopback RPC. Start it with
`yy serve --database metadata.db` and a separately supplied
`YINYANG_SERVICE_TOKEN`; do not expose its unencrypted transport to a network.

The [file runtime](specs/file-runtime.md) supports pinned handles, durable local
write/append/truncate, conditional remote fsync, and restart recovery over either
publication authority. Configure [volume admission](specs/volume-capabilities.md),
then use `yy --volume volume.json file ...` and `yy --volume volume.json status`.
Local write acknowledgment and remote publication are reported separately.
The file runtime's library frontend installs no mount; directory reconciliation
is the separate [Managed Sync](specs/linux-sync.md) frontend, and the
[Linux FUSE frontend](specs/linux-fuse.md) exposes Managed Mount instead.

## Previous releases

The [`backup`] branch preserves the implementation released as Apache OpenDAL™ File System.
Its Cargo package and command-line executable are both named `ofs`.
This historical implementation predates the YinYang redesign and remains available for reference.

[rfc-0016]: rfcs/0016_filesystem_architecture.md
[`backup`]: https://github.com/apache/opendal-yinyang/tree/backup

## Development

See [CONTRIBUTING.md] for setup instructions. Run the repository checks with:

```shell
cargo x lint
cargo x check
cargo x test
cargo x licenses
```

## Branding

Apache OpenDAL™ is the ASF project and umbrella brand. Apache OpenDAL™ YinYang software is a product developed by the Apache OpenDAL project.

Use the full product name, **Apache OpenDAL™ YinYang**, in titles and first prominent references. Later references may use **YinYang** when the relationship to the Apache OpenDAL project and the ASF remains clear.

Use `yinyang` only for the Cargo package and Rust crate, and use `yy` only for the command-line executable and commands.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>

Apache OpenDAL, OpenDAL, Apache OpenDAL YinYang, YinYang, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
