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

Run only the read-only range-hydration scenarios:

```shell
cargo x test-linux-fuse --read-only-range
```

## Scenarios

- Read-only basic: mounted reads, and explicit rejection (`EROFS`/`EACCES`) of
  writable open, create, mkdir and rename.
- Read-only range hydration: an 8 MiB file with a partial final verification
  unit must return correct 4 KiB, cross-unit, and final-unit pread results while
  allocated mount staging stays bounded instead of growing to the full file.
- Retained replacement descriptor: an open descriptor keeps its original bytes,
  length and inode after an independent remote replacement. SIGHUP exposes the
  replacement and a newly added file, while a fresh open observes the new node
  and the old descriptor remains pinned until it is closed.
- Corrupt content: independently flipping the content-index proof and a data
  extent must return `EIO` without releasing the requested bytes.
- Read-only staging admission: an interrupted writable mount with dirty bytes
  leaves unpublished staging for both authorities, and a later read-only mount
  must fail with a diagnostic naming that handle. A frozen service publication
  must likewise be rejected rather than exposed as a read-only snapshot.
- Read-only directories: nested `read_dir` enumeration and stable inode numbers
  across repeated stat calls, cached `readdir` after a file lookup/read, and a
  520-entry bulk directory that crosses the adapter's 512-entry readdir page.
- Basic writable I/O: create, write, remote `fsync` verified through an
  independent authority, append, truncate, and multiple descriptors on one file.
- Append flag toggling: one descriptor opened with `O_APPEND`, then cleared and
  restored via `fcntl(F_SETFL)`, must overwrite at an explicit offset while
  cleared and append only while restored; both remote results are verified.
- Dirty descriptor isolation: a read-only descriptor closed while another
  descriptor is dirty must not publish the dirty or frozen state; explicit
  `fsync` on the dirty descriptor establishes the remote result.
  Path stat and fd fstat both report the staged size after dirty grow/truncate
  across the attribute TTL, and an external grow/shrink cannot overwrite the
  same inode's staged size while an old descriptor is retained.
- CLOEXEC/flush boundary: while a parent keeps a dirty descriptor open, an
  in-process independent authority read still sees the old bytes; spawning
  `/bin/true` explicitly exec-closes the inherited descriptor and the next
  independent read observes the newly published bytes.
- External rename and add refresh: after warming a dentry and opening an old
  descriptor, an independent in-process authority renames `a` to `b`, changes
  `b`, and adds `new.txt`; SIGHUP must emit a native invalidation notification,
  the same mount must expose the new namespace, and the old descriptor (and a
  new open of the renamed path while it lives) must retain the shared pinned
  bytes and fstat until the old descriptor is closed; only the fresh open after
  that sees the independently observed longer payload. The grounded
  `yy: fuse refresh: done (inode=N entries=N errors=N)` line must report
  positive inode/entry counts and zero errors before namespace assertions;
  notification counts prove the native calls were accepted, while the
  namespace and pinned-fd assertions prove their effect.
- Path truncate: real `truncate(2)` without an open descriptor commits its
  shrink/extend result to the independent authority at an explicit boundary.
- Limited chmod: `0644 <-> 0755` is observable, while non-representable modes
  such as `0600` and `0000` fail with an explicit error and leave attributes
  unchanged. Directories remain fixed at `0755`; `chmod 0644` on a directory
  must not report success. A path chmod to executable while one descriptor is
  dirty must not self-conflict when that same descriptor continues writing and
  later performs `fsync`.
- Safe-save rename replacement: `rename` replaces an observed destination
  atomically and the independent authority observes the result; plain rename
  preserves the moved node's inode identity.
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
- Pending fsync interruption: a loopback TCP gate used only by the mount blocks
  the first connection observed after `Runtime::inspect` reports
  `frozen: true`. Killing the mount daemon then interrupts that pending
  publication, the authority is untouched, and a reopened mount replays the
  frozen request and publishes the staged bytes.
- Delayed completion with client interruption: a separate Python client opens
  and writes through `stdin`/`stdout` barriers, the gate blocks the frozen
  commit connection, and the client is SIGKILLed. After gate release the
  independent authority sees the delayed completion and the daemon remains
  usable and unmounts normally. This verifies the adapter's ignored
  interrupt/delayed completion behavior, not real cancellation.
- Busy unmount: `fusermount3 -u` fails while a descriptor is open and succeeds
  after the descriptor is released.
- Unmount lifecycle: after a successful external `fusermount3 -u`, `yy` exits
  on its own, removes the mount entry, and releases the staging lease. A
  separate clean SIGTERM scenario verifies the same process and lease
  lifecycle. Busy SIGTERM with an open descriptor or a pending fsync must exit
  through the forced non-zero grace path within 10s, leave the held descriptor
  with I/O errors, release the lease, and allow the same staging to remount.

The assertions follow the core specs. They do not add POSIX guarantees that the
transactional filesystem does not provide; for example symlinks are rejected,
and dirty bases report conflict rather than merging.
