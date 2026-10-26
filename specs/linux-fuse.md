# Read-only Linux FUSE frontend

**Status: experimental read-only Managed mount; protocol transport is the
`fuser` 0.17 binding, not a YinYang FUSE implementation.**

The Linux frontend mounts one Managed volume through the kernel FUSE ABI with
`yy --volume CONFIG mount MOUNTPOINT` and keeps the process in the foreground.
It uses the shared Authority, Mount and Runtime contracts directly; it never
initializes or reformats remote storage. The macOS bridge crate is not part of
the Linux path.

## Startup and admission

The volume configuration must declare `model = "managed"`, `access = "mount"`,
`frontend = "fuse"` and `read_only = true`. The shared admission rules in
[`volume-capabilities.md`](volume-capabilities.md) reject every other
combination before an authority connection or staging lease exists, and reject
the FUSE frontend entirely on non-Linux targets. Object and service publication
authorities are both accepted through `Volume::connect`.

Startup order:

1. validate the configuration and the storage capability intersection;
2. connect the object or service authority;
3. open the shared [`Mount`](mount-runtime.md) read-only (acquires the staging
   lease and runs the existing recovery);
4. inspect the recovered runtime status and refuse the mount if any handle is
   pending, frozen, conflicted or failed;
5. observe the latest remote snapshot and verify the root is a directory;
6. handshake with the kernel as `fuse.yinyang` and serve requests.

Step 4 is the read-only dirty-data boundary. Recovered clean observations are
disposable and reclaimed by existing Mount recovery, while unpublished staged
bytes are never exposed as a remote snapshot: the mount fails with a
diagnostic naming the handle instead. The lease is already held during the
inspection, so another client cannot interleave. Refusing startup changes no
remote state.

The mount is owner-only (`SessionACL::Owner`) and advertised as `ro` with
`fsname=yinyang-<name>` and `subtype=yinyang`, so the kernel reports
`fuse.yinyang` as the filesystem type.

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
metadata from two observations. Attributes are synthesized: directories
`0555`, files `0444` (readable plus the core executable bit), the mounting
user's uid/gid, size from the content descriptor or a fixed directory size,
and timestamps encoding the revision sequence rather than wall-clock time.
`getxattr`, `listxattr`, `readlink` and options outside the read-only contract
return explicit errors; mutations return `EROFS`.

`open` requires a read-only access mode, rejects cached directories with
`EISDIR`, and takes one reference on the node's shared staged version through
`Mount::open_node`. The reply opens with `FOPEN_DIRECT_IO`, so the kernel does
not cache file pages. Reads stream from the staged version. `getattr` with an
open handle reports the staged record's length, so `fstat` and read bytes
always describe the same pinned version even when the remote side changed;
`release` drops the reference and then reclaims a clean node, so the next open
materializes the current remote revision. A release whose reclaim loses to an
interrupted read leaves a clean references-zero record; `open` reclaims such a
record before materializing, so a fresh open still observes the current
revision instead of reusing the stale clean snapshot.

A clean node is only reclaimed when its last adapter reference is released. If
an interrupted read still shares the node, reclaim reports busy and the pinned
version remains coherent until a later release becomes the final reference;
that open's metadata and content never disagree. The frontend does not call
`Mount::refresh` while references exist, because refresh rebases live handles;
a writable layer that in-place refreshes must first define its open-fd and
kernel-cache invalidation policy.

## Explicit refresh

`SIGHUP` triggers one explicit refresh; this is a manual invalidation entry
point, not an automatic change watch. There is no background polling, no
change-feed watcher, and no whole-tree enumeration. The refresh observes the
latest snapshot once and then:

- reclaims every clean, unreferenced node through the shared `Mount` contract,
  so the next open materializes the current remote revision; nodes with live
  references or retained state are left pinned (`Busy`);
- invalidates known inode attributes with `inval_inode(ino, -1, 0)` and
  replied dentries with `inval_entry(parent, name)` through the fuser
  `Notifier`. Only dentries this adapter actually replied to the kernel are
  recorded: successful `LOOKUP` results. Negative lookups carry no timeout in
  FUSE replies and are not recorded, and plain `READDIR` does not install
  dentries without `READDIRPLUS`, so directory names are not recorded.

The adapter never calls `Mount::refresh`, because that replaces the shared
staged handle and would rebase open file descriptors. The refresh must also
hold no adapter lock while reclaiming nodes or sending notifications; targets
are collected under short locks, then the sends run on a blocking thread. At
most one refresh runs at a time and duplicate `SIGHUP`s while one is running
are coalesced by the signal stream. The SIGHUP loop is a tracked Tokio task:
the signal handler is registered synchronously before the mount becomes
visible, teardown sends it a stop signal, and the session join and the refresh
task share the same five-second grace period; a refresh already in progress
may finish inside that window. A `NotFound` notification result is expected
(the kernel already dropped the entry or inode) and is counted separately, so
only explicit connection-ended errors (`ENODEV`, `ENOTCONN`, `EPIPE`,
`ECONNRESET`, `ESHUTDOWN`) stop the batch before it would otherwise finish.

Stable `yy: fuse refresh:` log lines report the observed revision, the
reclaimed/active node counts, and the native notification counts at
completion, so an acceptance harness can confirm a refresh finished.

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

The binding answers `FUSE_INTERRUPT` with `ENOSYS` (fuser 0.17
`src/request.rs`), so an interrupted syscall does not cancel in-flight remote
work. The Linux kernel waits out every request already delivered to the daemon
(`fs/fuse/dev.c` `request_wait_answer`): a client terminated by a fatal signal
while its request is in userspace stays in an uninterruptible wait until the
reply arrives or the connection aborts, and only a request that was never
delivered can be dropped with `-EINTR`. The daemon therefore replies to every
request exactly once, and daemon termination is the abort guarantee: closing
`/dev/fuse` makes the kernel end pending requests with `-ECONNABORTED`
(`fuse_dev_release` -> `fuse_abort_conn`).

The session runs in the foreground. Teardown waits on both the session end and
SIGINT/SIGTERM: an external `fusermount3 -u` ends the session loop and the
process exits on its own, while a signal detaches the mount and waits up to a
five-second grace period. `Session::spawn` moves the mount object into
`BackgroundSession`, so a `SessionUnmounter` obtained before spawning would
find an empty slot; one blocking owner holds the background session, polls its
public thread guard for a natural end, and receives the local teardown
request. Exactly one `join` or `umount_and_join` runs. A lazy unmount does not
end the kernel connection while clients still hold open references or in-flight
requests, so when the grace period expires the mount entry returns an explicit
teardown-expired outcome instead of returning into a runtime shutdown that
would wait for the blocked session task. The CLI prints a diagnostic to stderr
and exits non-zero; the process exit closes `/dev/fuse`, the kernel aborts the
connection, and every blocked client syscall returns an error instead of
hanging. Forced termination is a crash-equivalent recovery boundary, not a
graceful unmount: the staging lease is released by the OS and the next start
recovers local state through the normal crash-recovery path. Request tasks are
detached and owned by the CLI runtime; their unanswered replies are dropped on
the closing channel. In-flight tasks hold their `Arc<Mount>` for that window;
there is no mid-session cancellation signal.

## Boundaries and known limits

- Every open materializes the complete file through the existing shared Mount
  staging contract; there is no partial-download or mmap coherence claim, and
  `FOPEN_DIRECT_IO` does not provide page-cache-backed mmap behavior.
- Writes, create, mkdir, unlink, rename, links, xattrs, ioctl, poll, locking
  and capacity accounting are not implemented; the kernel read-only mount and
  the callback errno policy reject them.
- The module keeps existing core Mount behavior, including the node-map lock
  that already spans first materialization; the adapter adds no mount-wide
  lock across authority or staging I/O.
- The session `Notifier` is used only by the explicit `SIGHUP` refresh above;
  there is no automatic external-change watch, and its capability set does
  not claim recoverable staging or any write-side operation beyond the
  read-only contract. Remaining extension points for the next layer are the
  runtime namespace operations and `Mount::refresh` guarded by reference
  counts.
