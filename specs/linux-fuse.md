# Linux FUSE frontend

**Status: experimental Managed mount; protocol transport is the
`fuser` 0.17 binding, not a YinYang FUSE implementation.**

The Linux frontend mounts one Managed volume through the kernel FUSE ABI with
`yy --volume CONFIG mount MOUNTPOINT` and keeps the process in the foreground.
It uses the shared Authority, Mount and Runtime contracts directly; it never
initializes or reformats remote storage. The macOS bridge crate is not part of
the Linux path.

## Startup and admission

The volume configuration must declare `model = "managed"`, `access = "mount"`
and `frontend = "fuse"`. Both read-only and writable mounts use the shared
admission rules in [`volume-capabilities.md`](volume-capabilities.md); the
frontend is rejected entirely on non-Linux targets. Object and service
publication authorities are both accepted through `Volume::connect`.

Startup order:

1. validate the configuration and the storage capability intersection;
2. connect the object or service authority;
3. open the shared [`Mount`](mount-runtime.md) using the configured write
   policy (acquires the staging lease and runs the existing recovery);
4. for a read-only mount, inspect the recovered runtime status and refuse the
   mount if any handle is pending, frozen, conflicted or failed;
5. observe the latest remote snapshot and verify the root is a directory;
6. handshake with the kernel as `fuse.yinyang` and serve requests.

Step 4 is the read-only dirty-data boundary. Recovered clean observations are
disposable and reclaimed by existing Mount recovery, while unpublished staged
bytes are never exposed as a remote snapshot: the mount fails with a
diagnostic naming the handle instead. A writable mount instead serves recovered
pending/frozen/error state through the shared Mount handles, so a later flush or
fsync reports the real remote outcome. The lease is already held during the
inspection, so another client cannot interleave. Refusing a read-only startup
changes no remote state.

The mount is owner-only (`SessionACL::Owner`) with `fsname=yinyang-<name>` and
`subtype=yinyang`, so the kernel reports `fuse.yinyang` as the filesystem type.
Only the read-only policy adds the kernel `ro` option.

## Identity and lookup references

Inode numbers are deterministic functions of the core `NodeId`: the root is
always 1 and every other node derives a 64-bit value from a domain-separated
BLAKE3 digest of its identity, so numbers are stable across mounts and
restarts. A derivation that collides with the root, zero, or an already mapped
identity is an error (`EIO`) rather than an alias. The NodeId-inode map and the
observed kind/executable cache live for the mount lifetime; memory is
proportional to the visited tree, the same boundary as the macOS prototype.

Each successful `LOOKUP` reply adds one kernel reference. `forget` subtracts
the reported count with saturating arithmetic. Mappings are retained after the
count reaches zero so a later deterministic lookup cannot change numbers; the
counters exist for a future eviction layer. FUSE reply generations are 0;
kernel NFS export is not supported.

## Attributes and pinned file handles

Filesystem callbacks that can touch the authority or staging dispatch onto the
process Tokio runtime, moving the reply into the task; the binding sends one
reply per request and replaces a dropped unanswered reply with `EIO`.

`lookup` and non-handle `getattr` observe the latest snapshot for metadata and
use that same snapshot's revision for timestamps, so a reply never mixes
metadata from two observations. Attributes are synthesized: read-only
directories are `0555` and files are `0444`; writable directories are `0755`
and files are `0644` or `0755` according to the core executable bit. The
mounting user's uid/gid, size from the content descriptor or a fixed directory
size, and revision-derived timestamps are synthesized. `getxattr`,
`listxattr`, `readlink`, links and special nodes return explicit unsupported
errors.

`open` validates the access mode, rejects cached directories with `EISDIR`, and
takes one reference on the node's shared staged version through
`Mount::open_node`. The reply opens with `FOPEN_DIRECT_IO`, so the kernel does
not cache file pages. Reads stream from the staged version. `getattr` with an
open handle reports the staged record's length, so `fstat` and read bytes
always describe the same pinned version even when the remote side changed;
`release` drops the reference and then reclaims a clean node, so the next open
materializes the current remote revision.

A clean node is only reclaimed when its last adapter reference is released. If
an interrupted read still shares the node, reclaim reports busy and the pinned
version remains coherent until a later release becomes the final reference;
that open's metadata and content never disagree. The frontend does not call
`Mount::refresh` while references exist, because refresh rebases live handles;
a writable layer that in-place refreshes must first define its open-fd and
kernel-cache invalidation policy. Before a new open, the adapter retries
reclaiming a clean unreferenced NodeId; a successful reclaim makes that open
materialize the latest remote version, while Busy retains all still-pinned or
dirty/frozen/error state.

## Writable callbacks

`write` stages bytes in the shared per-NodeId Mount handle. A handle opened with
`O_APPEND` uses the node's append path, which chooses its offset under the same
per-node lock as other writes; a regular write uses the kernel offset.
`O_TRUNC` stages a zero length before the open reply. `O_SYNC`/`O_DSYNC` writes
attempt publication before acknowledging the write. Unsupported write flags
return an explicit error instead of silently changing semantics.

`flush` publishes a writable handle and reports the real remote outcome to the
closing descriptor. `release` never publishes and never discards staging.
Flush or fsync on a read-only reference is a successful no-op even when another
writable reference shares the node, so a reader close cannot publish somebody
else's dirty bytes. A failed publication remains attached to the shared handle
and volume ledger; later fsync/flush retries the original frozen request.

`create` and `mkdir` use the Runtime helpers that return the NodeId produced by
the committing Planner, avoiding a post-commit path lookup. `unlink`, `rmdir`,
and path renames use the path-addressed Runtime methods, so lookup and commit
share one guarded Snapshot. Plain `rename` replaces an existing destination;
`RENAME_NOREPLACE` uses the no-replace form. Exchange and whiteout renames are
explicitly unsupported.

`setattr` supports size and the core executable flag only. Size on an open
descriptor is staged for that descriptor's later flush/fsync; path truncation
publishes before returning success. A mode request is accepted only when it is
one of the synthesized modes above (`0444`/`0555` or `0644`/`0755` according to
read-only policy and the executable bit); other permission combinations return
`EOPNOTSUPP`. Before changing the executable flag, the shared node lock
publishes dirty content, commits a Planner based on the handle's recorded
content revision, and advances only that clean handle to the receipt revision.
An external content change therefore produces Conflict rather than rebasing
old staged bytes onto new content. Ownership, timestamps and other mode bits
are not persisted.

## Pinned directory cursors

`opendir` pins one `Snapshot` revision and one directory identity. `readdir`
fills the kernel buffer from stable ordinal offsets: `.` at 1, `..` at 2, then
recorded entries in the snapshot's ordered scan, with each entry's returned
offset being the next ordinal. Pages of 512 entries are fetched lazily through
`Snapshot::scan` with the continuation bound to the pinned revision. Offset 0
rewinds the same pinned enumeration; a handle never silently switches to a
newer snapshot. Cursor state is serialized per open directory handle, and no
mount-wide lock spans a page scan or per-entry metadata read.

Each entry's inode and kind need the entry node's metadata; the frontend caches
observed kinds per NodeId, so listing a directory again after its children
were looked up does not re-read every node record. A fresh large listing still
performs one metadata read per entry plus small ordered pages; batched
metadata reads are a future optimization, not a completeness requirement.

## statfs, capacity and errors

`statfs` reports zero blocks, zero files and `bsize=4096` with `namelen=255`.
A remote snapshot implies no local capacity claim. Core and runtime failures
map to the narrow errno intended for each operation; unknown identity is
`ESTALE`, read-only violations `EROFS`, unsupported operations `ENOSYS` or
`EOPNOTSUPP`, and every other failure class degrades to `EIO` without guessing.

## Cancellation and exit

The binding answers `FUSE_INTERRUPT` with `ENOSYS`, and the frontend does not
negotiate any per-request cancellation extension. There is therefore no
per-request cancellation: once a FUSE request has been delivered to this
daemon, the kernel keeps waiting for its reply until it is answered or the
connection aborts, regardless of signals such as `SIGKILL` on the caller.
This frontend does not promise `EINTR` or that an interrupted caller returns
promptly. Each operation still completes and replies exactly once; teardown
only stops the request channel at process exit.

The session runs in the foreground. Teardown waits on both the session end and
SIGINT/SIGTERM: an external `fusermount3 -u` ends the session loop and the
process exits on its own, while a signal unmounts and joins the session before
exiting. `Session::spawn` moves the mount object into `BackgroundSession`, so a
`SessionUnmounter` obtained before spawning would find an empty slot; instead
one blocking owner holds the background session, polls its public thread guard
for a natural end, and receives the local teardown request. Exactly one `join`
or `umount_and_join` runs. Returning from `run` ends `main`, so the Tokio
runtime shuts down; request tasks are detached and owned by that runtime, their
unanswered replies are dropped on the closing channel, and the staging lease is
released at process exit. In-flight tasks hold their `Arc<Mount>` for that
window; there is no mid-session cancellation signal.

## Boundaries and known limits

- Every open materializes the complete file through the existing shared Mount
  staging contract; there is no partial-download or mmap coherence claim, and
  `FOPEN_DIRECT_IO` does not provide page-cache-backed mmap behavior.
- Hard links, symlinks, mknod, xattrs, ioctl, poll, fallocate and capacity
  accounting are not implemented and return explicit `EOPNOTSUPP`/`ENOSYS`.
- The frontend does not negotiate `FUSE_POSIX_LOCKS` or `FLOCK_LOCKS`, and its
  getlk/setlk/flock callbacks are not implemented. If the kernel still provides
  POSIX or flock locks, they are local to this mount instance and carry no
  cross-mount or cross-client meaning; no distributed lock semantics are
  claimed. A writable mount must not claim that locking is rejected outright,
  because the kernel may return success for its own local lock manager.
- The shared Mount node map is not held across first materialization or other
  authority/staging I/O; the adapter adds no mount-wide lock across those
  awaits.
- Extension points for the next layer are the runtime namespace operations,
  `Mount::refresh` guarded by reference counts, and the session `Notifier`
  for kernel entry/inode invalidation once an in-place refresh policy exists.
