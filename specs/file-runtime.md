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
access to the retained remote content, not a full local copy. Retain the NodeId
and revision and reopen through the same authority to recover this observation.

The recoverable `FileHandle` API below deliberately retains full materialization
for both read-only and writable handles, including its offline staged reads.
Use FileVersion for on-demand reads and provider downloads; use FileHandle for
durable writable staging. Opening FileVersion does not create a staging record.
Both paths use the same authenticated range reader. No remote format changes
or local staging migrations are required.

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
materializes the pinned file into verified 64 KiB staging chunks. Memory used
for materialization and upload is bounded by chunks; local disk use includes
the complete file. Opening is not a lazy read cache. Reads are relative to the
handle's pinned/staged version; another open observes a fresh version. Handles
are not automatically rebased onto concurrent writes.

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
| read(offset, length) | Return up to EOF from the opened/staged version. |
| write(offset, bytes) | Atomically stage bytes; gaps read as zeros. Empty writes do not extend the file. |
| append(bytes) | Append to this handle's private length, returning its offset. Concurrent handles do not merge appends. |
| truncate(length) | Shrink or zero-extend. Removed bytes never reappear after extension. |
| sync_local | Report local durable and remote published positions; does not publish. |
| flush / fsync / commit | Prepare bytes and conditionally publish their metadata; success means remote durability, not just local acceptance. |
| close | Publish dirty data, then release staging. Failure keeps the handle open/recoverable. |
| release | Consume the caller's handle reference and return its recovery UUID. Do not publish, delete staging, acknowledge errors, or resolve a frozen request. |
| abort | Discard a never-dispatched or definitively conflicted handle. Refuse an unresolved frozen request. |
| drop | Release the in-process lease only. Neither publish nor discard staging. |

Every successful write/append/truncate uses a synchronous SQLite transaction
for changed chunks and the local generation. This intentionally provides a
stronger local acknowledgment than a volatile writeback cache, at a per-write
sync cost. It does not implement offline namespace reconciliation or Sync.
The reported remote generation advances only after a committed remote receipt
and a durable local record update. A read-only or unchanged fsync need not
create a new remote commit.

Publication streams the staged file through Authority preparation, then plans
a conditional content update against the original base revision. It saves the
exact frozen transaction before dispatch. While frozen, new writes are
rejected. Retry uses the same identity, digest, and predicates, including after
process restart. Success updates the base to the receipt's revision; it never
silently observes a newer conflicting file.

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

The experimental local profile `yinyang-stage-2` stores filesystem binding,
handle records, chunk rows, and an error ledger in `staging.db` with WAL and
`synchronous=FULL`. The process lock is `runtime.lock`. Keep WAL with the
database; a copied live main database alone is not a supported backup.
Incomplete opens were never exposed and are discarded on reopen. Completed
handles and accepted writes survive process restart. There is no stable
migration promise for this local experimental format.

Opening stage-1 performs a single SQLite transaction that adds nullable
`failure BLOB` columns to handles and errors and advances the profile to
stage-2. Original handle records, chunks, frozen requests and ledger sequences
are preserved. Inspect supports either profile without upgrading it. Old
message-only failures report Unclassified; their kind and commit identity are
not inferred from prose. Older binaries reject stage-2; downgrade is unsupported.
The upgrade changes only local staging, not remote metadata or content.

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

## Validation and limits

The file-operation tests exercise public Runtime/FileHandle methods against
object and authenticated service authorities. They cover sparse gaps, append,
cross-chunk truncation/extension, pinned visibility, rename/unlink, conflicting
writers, failed uploads/close, frozen unknown requests, and reopening staging.
They also cover explicit reference release without publication, typed errors
through both authorities, retained Unknown identity, and stage-1 upgrades with
pending data and frozen requests. Range-reader tests verify bounded payload I/O
and corruption rejection without staging.
Production power-loss testing, capacity benchmarks, incremental upload
optimization, OS-specific mount behavior, and Sync reconciliation remain
separate work.
