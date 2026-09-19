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
> [RFC-0016]. The core supports persisted Managed filesystems and checked
> namespace edits. The experimental `yy` CLI publishes and restores directory
> snapshots. Mount and bidirectional Sync are not implemented yet.

## Publish and restore a directory

Build the experimental CLI with `cargo build --bin yy`. It currently connects
to S3-compatible storage. Create a bucket first, then configure OpenDAL through
`YINYANG_S3_*` environment variables:

```shell
export YINYANG_S3_BUCKET=my-bucket
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ROOT=my-filesystem
# For an S3-compatible service, also set YINYANG_S3_ENDPOINT.
# Supply credentials using YINYANG_S3_ACCESS_KEY_ID and
# YINYANG_S3_SECRET_ACCESS_KEY, or the OpenDAL AWS credential chain.

target/debug/yy create
target/debug/yy publish ./source
target/debug/yy status
target/debug/yy restore ./new-destination
```

Keep the source directory unchanged during publication. Publishing a later
complete snapshot requires `publish ./source --replace`: remote-only paths are
removed. Publication prints a commit UUID before transfer; retain it and pass
`--commit-id UUID` if the result is uncertain. Concurrent remote changes return
a conflict without automatic overwrite.

Restore requires a destination that does not exist. It verifies and installs
files individually without overwriting anything. An interrupted restore can
leave a partial directory; inspect it and restore into a new destination to
retry. This is one-shot transfer, not a background or bidirectional Sync engine.
See the [directory transfer contract](specs/directory-transfer.md) for supported
metadata, failure semantics, and limitations.

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
