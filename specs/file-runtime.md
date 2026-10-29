# File-operation runtime

**Status: experimental Managed runtime; no OS frontend or bidirectional Sync.**

`yinyang::runtime::Runtime` operates on an `Arc<dyn Authority>` and a local
staging directory. Object and service publication share exactly the same handle
lifecycle. Each directory is bound to one filesystem and held with an exclusive
OS file lock. Separate clients use separate staging directories. The local
filesystem must honor file locks and SQLite synchronization.

## Handles and visibility

`Snapshot::open_file(NodeId)` returns an immutable `FileVersion`, independent of
Runtime and staging. It reads only metadata when opening. `read(offset, length)`
returns up to EOF, including an empty result for zero length or offsets beyond
EOF. `read_range(range, destination)` streams an exact in-bounds range; invalid
ranges fail before payload I/O. Only intersecting verification units and proof
paths are read. Each unit is verified before delivery; a streaming error may
leave a verified prefix. A Vec-returning read exposes no partial result on error.

The version carries its observed node metadata, content descriptor and revision;
clones read that same content even after remote writes, rename or unlink. It has
no write methods, publication, staging lease or implicit rebasing. It needs
access to the retained remote content, not a full local copy. Ranges already
verified for this exact immutable version may be served from the binding's
bounded process-local cache; a cold miss still reads and authenticates remote
bytes. Retain the NodeId and revision and reopen through the same authority to
recover this observation.

The recoverable `FileHandle` API below pins an immutable `FileVersion` as its
base and stores only exact locally changed byte ranges. Untouched ranges are
authenticated and read from that pinned version on demand. Opening a FileHandle
creates durable local state but reads no file payload. Use FileVersion directly
when no writable staging record is required. Both paths use the same
authenticated range reader.

Identity queries and enumeration use the authority's `Snapshot::node`, `lookup`
and `scan` APIs. Keep one snapshot across directory pages: a continuation is
bound to its filesystem, directory and revision, and cannot be applied to a
fresh observation. NodeId is the identity; a path is only a convenience locator.

`open_node(id, writable)` opens a node at latest, even after a rename.
`open_node_at(revision, id, writable)` opens it in a retained revision validated
by this runtime's authority. It can read a historical node after unlink; writes
still conditionally publish against the original revision and cannot resurrect
it. A revision from another filesystem is rejected. Reusing an old path for a
new file never redirects an identity-based open to that new node.

`open_file(path, writable)` observes latest once, resolves a stable NodeId, and
pins its authenticated `FileVersion` without reading file payload. It records
the NodeId, revision and content descriptor, then marks the handle ready in one
SQLite transaction. Reads are relative to the handle's pinned base plus exact
local dirty ranges; another open observes a fresh version. Handles are not
automatically rebased onto concurrent writes. Recovering a sparse handle
requires the authority to retain and return that exact revision and descriptor.
Recovery and status do not contact that authority. A read covered entirely by
local dirty ranges or returning no bytes does not authenticate the base; a read
that needs untouched ranges authenticates it lazily, and any failure follows the
normal retained-handle error path.

A handle is uniquely leased in a runtime and is not cloneable. Its UUID can be
recovered after drop or process restart. Rename does not change its node
identity or invalidate content publication. Unlink does not invalidate staged
reads, but dirty publication after unlink returns Conflict and cannot resurrect
the name. This is retained-handle reading, not POSIX writable anonymous inodes.
There are no distributed locks, cross-handle atomic append, or multi-file
handle transactions.

Namespace create, mkdir, rename, and unlink are narrow conditional transactions.
They return a receipt or an explicit conflict/retryable/unknown result.
Unknown includes the commit ID for receipt lookup. These convenience namespace
methods do not persist a replay journal; durable replay applies to file-handle
publication. Callers needing frozen namespace replay use Authority/Planner.

`create_file_with_node` and `create_directory_with_node` return the NodeId
produced by the same Planner whose transaction returned the receipt. Adapters
must use that identity instead of resolving the path again after publication,
because a concurrent rename or replacement could otherwise bind the lookup to
another node. File creation accepts the core executable flag; no other POSIX
mode, ownership or timestamp state is persisted.

Path-addressed namespace helpers (`rename_path`, `rename_path_replace`,
`unlink_path`, and `rmdir_path`) resolve the requested entry and construct its
mutation from the same Snapshot. A path-based frontend must use these helpers
instead of resolving a NodeId in one observation and calling an
identity-addressed convenience, which observes latest again. If another client
moves the source node and replaces its old entry between observation and
publication, the transaction returns Conflict instead of deleting or moving the
replacement. The identity-addressed methods remain appropriate for an open
handle whose NodeId intentionally survives path changes.

`rename` never replaces a different destination node. `rename_replace` allows
replacement of an observed destination of the same kind, with an empty
directory required for directory replacement. Destination content, attributes,
link and directory membership are guarded: a concurrent destination edit,
replacement or new child conflicts. The source keeps its identity. Existing
versions and staged handles of the replaced node retain their bytes, but cannot
publish back to the removed identity. Same-node renames preserve identity and
allow case-only spelling changes. Replacing a file with a directory returns
NotDirectory; replacing a directory with a file returns IsDirectory.

The planner composes a guarded remove and rename in one frozen transaction,
without changing the request or remote storage format. No intermediate removal
is published. A failed `Planner::rename_replace` leaves the planner unchanged.
Like the other namespace conveniences, Runtime does not journal this operation
for cross-process replay; a caller requiring that guarantee persists the frozen
transaction before dispatch. This primitive supports safe-save adapters but
does not by itself provide a mounted editor workflow.

## Local writes and remote publication

| Operation | Contract |
| --- | --- |
| read(offset, length) | Return up to EOF by overlaying local ranges on the untouched pinned base; ranges beyond the valid base prefix read as zeros. |
| write(offset, bytes) | Atomically stage only the exact replaced bytes; gaps read as zeros. Empty writes do not extend the file. |
| append(bytes) | Append to this handle's private length, returning its offset. Concurrent handles do not merge appends. |
| truncate(length) | Shrink or zero-extend. Removed bytes never reappear after extension; a durable base-prefix limit fences later reads. |
| sync_local | Report local durable and remote published positions; does not publish. |
| flush / fsync / commit | Prepare bytes and conditionally publish their metadata; success means remote durability, not just local acceptance. |
| close | Publish dirty data, then release staging. Failure keeps the handle open/recoverable. |
| release | Consume the caller's handle reference and return its recovery UUID. Do not publish, delete staging, acknowledge errors, or resolve a frozen request. |
| abort | Discard a never-dispatched or definitively conflicted handle. Refuse an unresolved frozen request. |
| drop | Release the in-process lease only. Neither publish nor discard staging. |

Every successful write/append/truncate uses a synchronous SQLite transaction
for the exact changed byte segments and the local generation. This
intentionally provides a stronger local acknowledgment than a volatile
writeback cache, at a per-write sync cost. It does not implement offline
namespace reconciliation or Sync.
The reported remote generation advances only after a committed remote receipt
and a durable local record update. A read-only or unchanged fsync need not
create a new remote commit.

For a same-length update whose valid base prefix still covers the file,
publication turns ordered local dirty segments into fixed-offset
`ContentEdit` values and calls `Authority::prepare_overwrite` against the exact
pinned `FileVersion`. The fast path is limited to 4096 edits and 16 MiB of
encoded edit payload so memory and the authenticated service frame stay
bounded. Append, truncate, shrink-then-extend, larger update sets, and
length-changing writes use the existing streaming `Authority::prepare` path.

Publication then plans a conditional content update against the original base
revision and saves the exact frozen transaction before dispatch. While frozen,
new writes are rejected. Retry restores that same identity, digest, descriptor,
and predicates, including after process restart; it does not reprepare or
rebase. Success updates the durable sparse base to the receipt's revision. It
never silently observes a newer conflicting file.

A stale file generation, changed executable state, or unlink produces Conflict.
Staged bytes remain readable for explicit recovery/export or abort. Retryable
and Unknown preserve the frozen request. Receipt absence cannot authorize
aborting it. A crash between remote commit and local acknowledgment therefore
recovers by replaying the original request and reading its original receipt.

## Staging and errors

Local acceptance, remote completion and reference release are separate events.
After release, a caller may immediately recover the UUID in the same runtime;
after restart, the same ID recovers the durable state. Release initiates no
publication or staging mutation and makes no remote completion claim. Even a clean released handle stays in
staging until explicit close or abort. A failed close can be followed by release
without losing bytes, the frozen request, or the error ledger.

`Error::kind()` exposes domain categories, not platform status codes. Namespace
failures distinguish NotFound, AlreadyExists, InvalidName, NotDirectory,
IsDirectory and NotEmpty. Runtime failures distinguish ReadOnly, Busy, Closed,
Frozen and TooLarge; publication distinguishes Conflict, Retryable and Unknown.
Corrupt, Unsupported, Storage, Io, PermissionDenied and NoSpace preserve the
available failure category without parsing messages. Invalid covers other
contract violations; unknown backend failures remain Storage/Io rather than
being guessed as conflicts or safe retries. `Error::commit_id()` retains the
original Unknown publication identity.

`HandleStatus::error` and `WritebackError::error` contain a typed `Failure`
with kind, diagnostic message and optional commit ID. Clean fsync/close returns
the retained category until acknowledgment. Clearing the volume ledger does not
clear an active handle's error, pending data or frozen request.

The current experimental local profile `yinyang-stage-3` stores filesystem
binding, handle records, the error ledger, sparse base descriptors, and exact
dirty segments in `staging.db` with WAL and `synchronous=FULL`. The process
lock is `runtime.lock`. Keep WAL with the database; a copied live main database
alone is not a supported backup. Incomplete opens were never exposed and are
discarded on reopen. Completed handles and accepted writes survive process
restart. There is no stable migration promise for this local experimental
format.

The supported schema signatures are exact table sets and ordered columns.
Stage-1 has `authority`, `handles`, `chunks`, and `errors` without typed-failure
columns. Stage-2 adds the nullable `failure` column after `record` in `handles`
and after `message` in `errors`. Stage-3 adds `bases(handle, descriptor,
prefix)` and `segments(handle, position, data)` while retaining `chunks` for
older full-materialization records. Column order is part of the signature
because maintained writers use positional inserts. SQLite internal tables are
excluded; primary-key definitions, declared types, and other constraints are
not treated as an independent signature.

A stage-3 handle with a `bases` row uses only `segments`; missing chunk rows
mean "read the pinned base", never zero and never a freshly observed version.
`prefix` is the durably valid immutable-base length and cannot exceed the
descriptor length. Writes replace exact byte intervals and coalesce adjacent
segments only inside one fixed 64 KiB verification unit. Every persisted
segment row is therefore at most 64 KiB and never crosses a unit boundary;
sequential writes cannot grow one SQLite BLOB without bound, and a small read
loads at most the intersecting unit rows. A shrink lowers `prefix`; later
extension therefore reads zeros in the removed interval instead of
resurrecting base bytes. A handle without a `bases` row retains stage-1/2
full-chunks semantics, where missing chunks are zeros. A stage-3 handle must
not contain both semantics.

A writer preflights an existing database read-only. A supported stage-1 or
stage-2 database is first fully decoded, then one SQLite transaction performs
the required typed-failure columns, adds the sparse tables, and advances the
profile to stage-3. Original handle records, chunks, frozen requests,
CommitIds, and ledger sequences are preserved. Old pending/frozen records keep
their original full-chunks reads and publication requests. Unknown profiles,
partial schemas, malformed base descriptors, overlapping segments, or mixed
full/sparse handle state, empty segments, and segments that cross a verification
unit are rejected before PRAGMA journal-mode configuration, DDL, runtime-lock
creation, or incomplete-open cleanup. SQLite's standard
read-only WAL path may create an empty `-wal` and may create or update derived
`-shm`. Those files are not logical staging records: the main database, any
existing WAL/journal bytes, configured journal mode, schema, rows, lease, and
cleanup state remain unchanged.

An absent or zero-table database is initialized only after the writer lease is
held. A writer that finishes between preflight and lease acquisition may have
already performed a supported upgrade; the lock holder revalidates and accepts
that state rather than requiring an unchanged preflight profile.

Inspect and backup validation accept stage-1, stage-2 or stage-3 without upgrading,
changing authoritative database/WAL bytes or journal mode, cleaning handles, or
acquiring the writer lease. SQLite may create an empty `-wal` and may create or
update derived `-shm`; existing WAL/journal bytes remain protected. Old
message-only failures report Unclassified; their kind and commit identity are
not inferred from prose. A frozen plan still exposes its exact CommitId.
Older binaries reject stage-3; downgrade is unsupported. The upgrade changes
only local staging, not remote metadata or content. A sparse handle cannot read
an untouched range if its authority no longer retains the exact base revision.

Failure blobs use Borsh `(kind: u8, message: String, commit: Option<[u8;16]>)`.
Kind tags, in order from 0 through 21, are Invalid, InvalidName, NotFound,
AlreadyExists, NotDirectory, IsDirectory, NotEmpty, ReadOnly, PermissionDenied,
Busy, Closed, Frozen, TooLarge, Conflict, Retryable, Unknown, Corrupt, Unsupported,
Storage, Io, NoSpace and Unclassified. New handle records keep the original
message field as a diagnostic copy; typed details live in the adjacent column.
An operation failure records both fields and its ledger entry atomically.

Failures are returned immediately and retained on the handle and volume ledger.
A successful retry clears the active handle error, not the ledger. A clean
handle with a retained error returns it on fsync/close until
`acknowledge_error`. `acknowledge_errors(sequence)` clears only acknowledged
ledger entries. If a local disk failure also prevents persisting the error,
the running process retains it in memory and marks it non-persisted; no claim
is made that an unwritable disk can preserve that error across restart.

Status exposes each handle's local generation, remote generation and revision,
pending/frozen/conflict state, and last error. Durability is the acknowledgment
of the configured filesystem/backend, not a claim of stronger physical-media
guarantees.

`HandleStatus::frozen_commit_id` exposes the exact identity of a persisted
frozen transaction. It is derived read-only from the local plan through
`Transaction::inspect_identity`, so it survives restart and requires neither
network access nor the runtime lease. A generic response or transport error
after dispatch is reported as `Unknown`: the original diagnostic and commit ID
remain available, and retry uses the same frozen request rather than treating
the missing response as a definitive failure.

For a quiesced backup, stop the file or Mount owner and create a consistent
SQLite backup of `staging.db`. Base descriptors, dirty segments, legacy chunks,
frozen requests, generations, and the error ledger are all in that database;
`runtime.lock` is only a process lease.
See [Linux backup and isolated recovery](../docs/linux-backup-recovery.md).

## Validation and limits

The file-operation tests exercise public Runtime/FileHandle methods against
object and authenticated service authorities. They cover exact sparse segments,
multi-fd reads, append, cross-chunk truncation/extension, shrink-then-extend
zeroing, pinned visibility, rename/unlink, conflicting writers, failed
uploads/close, frozen unknown requests, and reopening staging. They also cover
explicit reference release without publication, typed errors through both
authorities, retained Unknown identity, stage-1/2 upgrades with pending data and
frozen requests, and unchanged full-chunks semantics for old records.
Range-reader tests verify bounded payload I/O and corruption rejection without
staging.
Production power-loss testing, multi-host capacity benchmarks, OS-specific
mount behavior, and Sync reconciliation remain separate work.
