# Linux FUSE frontend

**Status: experimental Managed and Direct mounts; Direct defaults read-only and
is writable only with explicit local staging. Protocol transport is the `fuser`
0.17 binding, not a YinYang FUSE implementation.**

The Linux frontend mounts one Managed or Direct volume through the kernel FUSE
ABI with `yy --volume CONFIG mount MOUNTPOINT` and keeps the process in the
foreground.
One `Filesystem` adapter owns the session lifecycle, inode table, directory
cursors, attribute synthesis, flag handling and errno mapping. Managed and
Direct implement one backend contract for the operations that genuinely differ.
Managed uses the shared Authority, Mount and Runtime contracts directly; Direct
uses its storage-native snapshot and file-pin contract. Neither initializes or
reformats remote storage, and the macOS bridge crate is not part of the Linux
path.

## Startup and admission

The volume configuration must declare `access = "mount"` and
`frontend = "fuse"`. Managed accepts read-only and writable policies and
object or service publication. Direct accepts read-only object publication by
default and writable object publication only when `read_only=false` and a
staging path are explicitly configured.
Both use the admission rules in [`volume-capabilities.md`](volume-capabilities.md);
Direct's mapping, pin and error contract is in [`direct.md`](direct.md). The
frontend is rejected entirely on non-Linux targets.

Startup, teardown and backend-open failures return `fuse::Error` with one
`crate::runtime::ErrorKind`. The CLI consumes that typed kind directly; it does
not inspect or downcast a boxed error chain.

Managed startup order:

1. run the shared admission table and validate the storage prerequisites;
2. connect the object or service authority;
3. open the shared [`Mount`](mount-runtime.md) using the configured write
   policy (acquires the staging lease and runs the existing recovery);
4. for a read-only mount, inspect the recovered runtime status and refuse the
   mount if any handle is pending, frozen, conflicted or failed;
5. observe the latest remote snapshot and verify the root is a directory;
6. handshake with the kernel as `fuse.yinyang` and serve requests.

Read-only Direct startup validates the complete configuration, creates no
staging or Managed authority, performs one read-only recursive object
observation, and then hands the accepted snapshot to the kernel. Recursive LIST
is not an atomic multi-page namespace transaction. Writable Direct validates
the same admission and additionally binds the existing staging directory to
the selected storage namespace before loading retained objects. It then
observes the remote namespace and serves durable local-only, dirty, conflict
and Unknown object states as an overlay. Opening or recovering a writable
Direct mount does not create YinYang Managed metadata.

LOOKUP and path GETATTR reuse the accepted snapshot plus that overlay. Direct
has no periodic re-listing: a recursive LIST of the whole namespace is too
expensive to run on a metadata callback. Its accepted snapshot changes only at
startup and on an explicit SIGHUP, which is the documented freshness exception.
SIGHUP replaces the remote snapshot without rebasing open descriptors. Direct
inode identity includes the accepted object version or ETag, size and
whole-second mtime, so a later observation of replaced content receives a new
inode and an old descriptor keeps its accepted attributes and pinned ranges.
Pending local identities remain stable until they acquire a new remote
observation. The complete Direct storage contract is in
[`direct.md`](direct.md).

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

One inode table serves both backends. The root is always 1 and every other node
derives a 64-bit value from a domain-separated BLAKE3 digest of the backend
identity. Managed identity is a NodeId. Direct identity includes the accepted
path, kind, version or ETag, size and whole-second mtime. The domains are
different, so Managed and Direct inode numbers are stable across mounts without
pretending the identities are interchangeable. A derivation that collides with
the root, zero, or an already mapped identity is an error (`EIO`) rather than an
alias. The table and observed kind/executable cache live for the mount lifetime.

Each successful `LOOKUP` reply adds one kernel reference. `forget` subtracts
the reported count with saturating arithmetic. Mappings are retained after the
count reaches zero so a later deterministic lookup cannot change numbers; the
counters exist for a future eviction layer. FUSE reply generations are 0;
kernel NFS export is not supported.

## Attributes and pinned file handles

Filesystem callbacks that can touch the authority or staging dispatch onto the
process Tokio runtime, moving the reply into the task; the binding sends one
reply per request and replaces a dropped unanswered reply with `EIO`.

Managed `lookup` and non-handle `getattr` reuse one accepted snapshot for a
one-second metadata staleness window. Repeated callbacks inside that window do
not observe the authority head again, and a cached IndexCache page or external
value may be shared across successive snapshots of the same immutable
authority. `open` still observes the latest revision; a mutation through the
mount installs its post-commit snapshot or invalidates the cached one so a
following metadata callback observes the result. Direct uses its accepted
snapshot until SIGHUP as described above. Attributes are synthesized: read-only
directories are `0555` and files are `0444`; writable directories are `0755`
and files are `0644` or `0755` according to the core executable bit. The
mounting user's uid/gid, size from the content descriptor or a fixed directory
size, and revision-derived timestamps are synthesized. `getxattr`,
`listxattr`, `readlink`, links and special nodes return explicit unsupported
errors.

The object authority's shared index cache charges index pages and external
values against one 64 MiB byte bound and evicts the oldest-used entry. Cached
entries are content-addressed `PackedRef` values, so a snapshot cannot consume
bytes from another content identity. Writers do not use this shared cache.

Direct has no persisted executable bit: writable Direct files remain `0644`
and directories remain `0755`. Writable Direct uses a zero attribute TTL and
explicit invalidation after publication so a dirty length cannot remain cached
after remote replacement. A pending overlay entry supplies its stable local
identity, length and state until a later remote observation resolves it.

`open` validates the access mode and rejects cached directories with `EISDIR`.
The reply opens with `FOPEN_DIRECT_IO`, so the kernel does not cache file pages.
The pinned state depends on the volume write policy.

For a read-only whole-volume mount, `Mount::open_read_only_node` observes latest
and pins the authenticated `Snapshot::open_file` result without creating a
staging record or reading the complete payload. Concurrent opens of one NodeId
share that immutable `FileVersion`; a later remote revision does not rebase an
existing descriptor. `read` asks the `FileVersion` range reader for only the
intersecting authenticated content units. `getattr` with the handle, and path
attributes while that observation is active, use its node metadata, executable
bit, length and revision. `release` drops the reference and reclaims a
zero-reference observation, so the next open pins the current remote revision.
Repeated reads may reuse previously verified proof nodes and units from the
content binding's bounded process-local cache; cache keys are bound to the exact
immutable version and `PackedRef`, and a fresh version cannot consume them.
Before replying to a read-only open, the frontend sends
`inval_inode(ino, -1, 0)` so a cached pre-transition attribute cannot survive
while READ uses the newly pinned version. The open reply becomes visible only
after the notification send has completed. If the notifier is unavailable or
the kernel cannot accept that invalidation, OPEN fails rather than exposing a
descriptor whose first fstat can combine the new content with a cached old
length. The notifier is installed from the session channel before the request
loop starts.
If an interrupted read still owns the observation after release, reclaim is
`Busy`; a later open retries reclaim before observing latest, preserving the
old handle until its operation finishes without reusing a stale zero-reference
entry indefinitely. A path metadata lookup also retires a zero-reference
observation under the Mount map lock, so it falls back to a fresh latest
observation instead of continuing to report the finished operation's version.

For writable volumes, both writable descriptors and read-only descriptors take
one reference on the node's shared staged version through `Mount::open_node`.
This keeps a reader on the same accepted dirty view as other descriptors.
Reads overlay exact staged ranges on the node's pinned authenticated base, and
`getattr` with an open handle reports the staged record's length, so `fstat` and
read bytes always describe the same version even when the remote side changed.
Release and fresh-open rules retain the staged-node behavior: only the last
clean reference is reclaimed, and a references-zero result left by an
interrupted operation is reclaimed before a later open captures current remote
metadata.

FUSE RELEASE removes the adapter handle and drops its backend file reference on
the dispatch thread before its asynchronous clean reclamation. For a Managed
volume this lowers the Mount reference count before the kernel can dispatch the
next request, so a following OPEN reclaims the zero-reference observation and
uses current remote metadata. Linux can process ordinary RELEASE work in the
background, so an application close returning does not prove this callback has
run. After the callback is dispatched, a request task that already cloned the
handle still retains the existing Busy pin until that operation finishes. A
later OPEN after reclamation observes current remote metadata. Before replying
to a writable OPEN, after selecting the Mount handle and applying `O_TRUNC`, the
adapter sends the same inode attribute invalidation used by read-only opens;
notification failure fails OPEN rather than exposing a descriptor whose first
fstat can use a cached old length.

A clean node is only reclaimed when its last adapter reference is released. If
an interrupted read still shares the node, reclaim reports busy and the pinned
version remains coherent until a later release becomes the final reference;
that open's metadata and content never disagree. The frontend does not call
`Mount::refresh` while references exist, because refresh rebases live handles;
a writable layer that in-place refreshes must first define its open-fd and
kernel-cache invalidation policy. Before a new staged open or a staged path
attribute, the adapter retries reclaiming a clean unreferenced NodeId; a
successful reclaim makes the next observation use the latest remote version,
while Busy retains all still-pinned or dirty/frozen/error state.

## Writable callbacks

`write` stages only the exact replaced byte ranges in the shared per-NodeId
Mount handle. Each request
evaluates the file flags supplied by the kernel. `fcntl(F_SETFL)` can toggle
`O_APPEND` for later writes. `O_SYNC`/`O_DSYNC` are set at open or carried as
per-request IOCB sync markers; `F_SETFL` does not toggle them. An append
request uses the node's append path, which chooses its offset under the same
per-node lock as other writes; a regular write uses the kernel offset. The
writable access mode is fixed by the open request. `O_TRUNC` stages a zero
length before the open reply. A write whose current flags include
`O_SYNC`/`O_DSYNC` attempts publication before acknowledging the write.
Unsupported write flags return an explicit error instead of silently changing
semantics.

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
`EOPNOTSUPP`. Linux may include `S_IFREG`/`S_IFDIR` type bits in the mode;
they are accepted when they match the node, while setuid/setgid/sticky and
other permission combinations are rejected. Kernel bookkeeping timestamps
attached to a chmod or truncate request are accepted and ignored, and the reply
always carries revision-derived timestamps. Standalone timestamp changes and
ownership changes remain explicitly unsupported. Before changing the
executable flag, the shared node lock
publishes dirty content, commits a Planner based on the handle's recorded
content revision, and advances only that clean handle to the receipt revision.
An external content change therefore produces Conflict rather than rebasing
old staged bytes onto new content. Ownership, timestamps and other mode bits
are not persisted.

### Direct writable callbacks

A writable Direct mount uses the storage-bound whole-object runtime in
[`direct.md`](direct.md). Files are synthesized as `0644` and directories as
`0755`; chmod, ownership, timestamps and other metadata remain unsupported.
Opening a file for writing materializes the pinned object into durable local
staging. All descriptors for one raw object key share that staged generation,
so a read-only descriptor observes the same dirty bytes and length as a writer.
READ never switches to a newer remote version while either descriptor is open.

`CREATE` publishes only a local empty object into the namespace overlay. A
writable descriptor publishes it on flush/fsync/close; a descriptor opened
without write access publishes the empty object before CREATE returns so its
creation does not depend on a later writer. `O_TRUNC` stages zero bytes before
OPEN returns. Each `WRITE` re-reads the kernel-supplied flags, so `F_SETFL`
changes `O_APPEND` for later requests and `O_SYNC`/`O_DSYNC` publish before the
write is acknowledged. Empty writes are no-ops.

For Direct, FLUSH and FSYNC are the publication boundary: they freeze the
current bytes and conditionally publish that exact generation. A successful
write response records the response pin; a later external replacement is not
silently adopted. Conflict leaves the local bytes and an `ESTALE` result;
ambiguous completion remains Unknown and returns `EIO`; unsupported conditional
publication returns `EOPNOTSUPP`. RELEASE only drops the in-process reference,
never publishes, and never discards retained bytes. A failed publication is
retryable through the shared runtime, and restart restores the same dirty,
conflict or Unknown generation.

`SETATTR` size with an open writable handle stages a truncate for that handle's
later flush. Size without a handle opens the current object, stages the
truncate, publishes it and returns only after the conditional result is known.
Mode-only, ownership, standalone timestamp and unsupported combination
requests fail with `EOPNOTSUPP`.

`UNLINK` uses the runtime's conditional object deletion; it never falls back to
an unconditional delete. File rename follows the runtime's non-atomic
safe-save sequence: conditionally publish the destination, then conditionally
delete the source. A failure after destination publication retains the
destination and source and reports `EIO` with PendingSourceCleanup/Unknown
rather than claiming atomic rename. `RENAME_NOREPLACE` is supported; `RENAME_EXCHANGE`,
`RENAME_WHITEOUT`, directory rename and directory creation/removal are
unsupported with explicit errors. A normal editor write-in-place save works;
write-temp-then-rename is supported only through this safe-save sequence, not
as a cross-object atomic guarantee.

## Explicit refresh

`SIGHUP` triggers one explicit refresh in the shared session lifecycle; this is
a manual invalidation entry point, not an automatic change watch. There is no
background polling and no change-feed watcher. Managed refresh performs one
latest observation; Direct performs one recursive namespace observation because
that is the only way to produce a new complete Direct snapshot. The refresh then:

- reclaims every clean, unreferenced node through the shared `Mount` contract,
  including zero-reference read-only range observations, so the next open
  observes or materializes the current remote revision; nodes with live
  references or retained state are left pinned (`Busy`);
- for writable Direct, reloads durable pending object and mutation status and
  keeps those names ahead of the new remote snapshot; open descriptors remain
  attached to their existing local generation;
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

Namespace callbacks do not issue synchronous `inval_entry` notifications before
replying. The kernel already updates the dentry involved in CREATE, UNLINK or
RENAME; a reverse entry invalidation from inside that request can deadlock while
the kernel holds the related parent lock. Callback-side refresh updates only the
adapter snapshot and pending overlay. Metadata-only inode invalidation and
dentry invalidation remain confined to the explicit SIGHUP path, outside a
related mutation request.

An `unlink` or `rename` precondition conflict does not install a refreshed
remote snapshot and returns `EAGAIN`, not `ESTALE`, for both backends. Linux
namei automatically retries `ESTALE` once inside the same syscall; a concurrent
global refresh could make that retry use the external source or destination pin
as a new intent. `EAGAIN` prevents that implicit retry and leaves the decision
to the caller.

The pending Conflict observation remains visible, and the failed callback does
not install a refreshed snapshot. A caller can explicitly refresh with `SIGHUP`
and then issue a new operation. Any later caller-owned request is a new intent
and may observe a later accepted global snapshot, including one advanced by an
unrelated successful mutation. Lookup or handle failures that cannot revalidate
identity continue to report `ESTALE`.

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
A remote snapshot implies no local capacity claim. The adapter has one errno
table for core, runtime and Direct failures. Unknown Direct identity is
`ESTALE`; read-only violations are `EROFS`; unsupported operations are `ENOSYS`
or `EOPNOTSUPP`; unlink/rename precondition conflicts are `EAGAIN`; and every
other failure class degrades to `EIO` without guessing.

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
process exits on its own, while a signal detaches the mount and waits up to a
five-second grace period. SIGINT and SIGTERM handlers, like the SIGHUP handler,
are registered before the mount becomes visible, so a signal that arrives right
after the mount appears detaches it instead of killing the process. `Session::spawn` moves the mount object into
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
the closing channel. In-flight tasks hold the mount's shared filesystem state
for that window; there is no mid-session cancellation signal.

## Boundaries and known limits

- A writable Managed open pins the selected authenticated base and stages only
  exact dirty ranges; untouched reads fetch authenticated intersecting base
  ranges. A writable Direct open materializes its raw object. A read-only
  whole-volume open pins immutable metadata and reads only authenticated
  intersecting content ranges. None of these paths claims mmap coherence, and
  `FOPEN_DIRECT_IO` does not provide page-cache-backed mmap behavior.
- Direct does not claim Managed stable identity, retained unlink reads,
  Managed receipts, cross-object transactions, or atomic rename. External
  replacement changes the next remote inode; open descriptors never rebase.
  Direct directories are synthesized from object key prefixes, so MKDIR/RMDIR
  and directory rename are outside the writable contract.
- Hard links, symlinks, mknod, xattrs, ioctl, poll, fallocate and capacity
  accounting are not implemented and return explicit `EOPNOTSUPP`/`ENOSYS`.
- The frontend does not negotiate `FUSE_POSIX_LOCKS` or `FLOCK_LOCKS`, and its
  getlk/setlk/flock callbacks are not implemented. If the kernel still provides
  POSIX or flock locks, they are local to this mount instance and carry no
  cross-mount or cross-client meaning; no distributed lock semantics are
  claimed. A writable mount must not claim that locking is rejected outright,
  because the kernel may return success for its own local lock manager.
- Node cache lookup and open-gate registration share a short-lived node-map
  critical section. First Managed base capture, Direct materialization, or
  range pinning is serialized per NodeId by that gate; neither lock is held
  across remote observation or staging I/O, and different nodes initialize
  independently.
- The session `Notifier` serves explicit `SIGHUP` refresh invalidation and the
  pre-open inode attribute invalidation required by read-only range opens.
  There is no automatic external-change watch.
