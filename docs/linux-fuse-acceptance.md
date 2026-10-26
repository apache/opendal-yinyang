# Linux FUSE acceptance

This is the acceptance entry point for the Linux mount frontend. It validates
the real product boundary: a `yy` subprocess installs a `/dev/fuse` mountpoint
through `yy --volume CONFIG mount MOUNTPOINT`, and every scenario operates on
that mountpoint with ordinary POSIX file descriptors. A mount that is not the
YinYang kernel filesystem type (`fuse.yinyang`), a missing `/dev/fuse`, missing
S3 settings, or a failed subprocess is a hard failure. There is no skip path in
this acceptance run.

The regular workspace checks (`cargo x test`) do not depend on `/dev/fuse`;
`tests/linux_fuse` is compiled on Linux but its scenarios stay ignored there.
Both object and service publication are exercised.

## Run

Use the same MinIO-compatible environment as the S3 CI job:

```shell
export YINYANG_S3_BUCKET=yinyang-test
export YINYANG_S3_ENDPOINT=http://127.0.0.1:9000
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ACCESS_KEY_ID=yinyang-test
export YINYANG_S3_SECRET_ACCESS_KEY=yinyang-test-only
export YINYANG_STORAGE_PROFILE=minio

cargo x test-linux-fuse
```

Start an isolated Silo (the CI workflow uses the pinned `pgsty/silo` image)
before running locally. Every test creates its own remote prefix and staging
directory, mounts under its own temporary directory, and cleans up with
`fusermount3`.

Run only the read-only stage for a first-layer adapter PR:

```shell
cargo x test-linux-fuse --read-only
```

## Scenarios

- Read-only basic: mounted reads, and explicit rejection (`EROFS`/`EACCES`) of
  writable open, create, mkdir and rename.
- Basic writable I/O: create, write, remote `fsync` verified through an
  independent authority, append, truncate, and multiple descriptors on one file.
- Safe-save rename replacement: `rename` replaces an observed destination
  atomically and the independent authority observes the result.
- Retained unlink reads: bytes stay readable through an already-open descriptor
  after `unlink`.
- Unsupported operations: symlink and hard link return an explicit OS error
  instead of advertising POSIX links.
- External changes and dirty refresh: a descriptor opened before an external
  grow/shrink keeps its pinned size, EOF and pread results, a fresh reopen
  reflects the authoritative size, and a dirty descriptor keeps its staged
  bytes while `fsync` reports failure instead of silently rebasing.
- Concurrent mounts: two live `yy mount` processes on separate staging
  directories publish distinct files concurrently under a barrier, then read
  across each other's mounts.
- Process kill recovery: `SIGKILL` of the mount process retains staged bytes
  through persistent staging while the descriptor is still open; a reopened
  mount can read and publish them.
- Fsync/network failure retention: stopping the metadata service makes
  `fsync` fail within a bound; staged bytes survive, the service restarts, and
  a reopened mount publishes them through the unchanged service address.
- Interrupted call retention: a `fsync` left in flight against a paused
  metadata service is interrupted by killing the mount daemon, the authority is
  untouched, and a reopened mount publishes the staged bytes.
- Busy unmount: `fusermount3 -u` fails while a descriptor is open and succeeds
  after the descriptor is released.
- Unmount lifecycle: after a successful external `fusermount3 -u`, `yy` exits
  on its own, removes the mount entry, and releases the staging lease. A
  separate SIGTERM scenario verifies the same process and lease lifecycle.

The assertions follow the core specs. They do not add POSIX guarantees that the
transactional filesystem does not provide; for example symlinks are rejected,
and dirty bases report conflict rather than merging.
