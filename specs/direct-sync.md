# Linux ordinary-directory Direct Sync

**Status: experimental, Linux-only, finite explicit reconciliation.**

`yinyang::direct_sync::DirectSync` reconciles one ordinary local directory with
one storage-native Direct object namespace. It owns a durable local baseline,
pending publication state, local installation journal, conflicts and retained
artifacts. It does not create Managed NodeIds, CommitIds, transactions, receipts
or authoritative YinYang metadata. It does not provide stable identity across
object replacement.

Direct Sync uses the stateless conditional mutation primitives in
[`direct.md`](direct.md). It is not based on a Mount handle and does not share
the Managed Sync receipt model.

## Admission

Direct Sync requires a Linux host and this configuration combination:

```json
{
  "name": "objects",
  "model": "direct",
  "access": "sync",
  "frontend": "library",
  "publication": "object",
  "storage_profile": "minio",
  "staging": "/var/lib/yinyang/objects-sync",
  "read_only": false
}
```

The existing `staging` field is the Direct Sync state directory. It is not
object payload staging for a Managed runtime. It must be non-empty, outside the
local root, on the same filesystem as the local root, and writable only as
required by the installing account. The state directory must not grant group or
other permissions. It is dedicated to one Direct Sync binding and must not be
shared with a writable Direct Mount runtime or another Sync root.

Admission runs before state creation or storage effects. In addition to the
read prerequisites in [`direct.md`](direct.md), Direct Sync requires:

- whole-object write;
- empty-object write;
- conditional creation with `if-not-exists`;
- conditional replacement using exact ETag `If-Match`;
- conditional delete using exact ETag `If-Match`.

A backend missing conditional replacement or conditional delete is rejected as
a Direct Sync configuration. Version IDs strengthen reads, but replacement and
delete conditions use ETags; Direct Sync does not claim version-only
compare-and-swap semantics. ETag compare-and-swap has the backend's normal
same-ETag/ABA limit: an external transition away from and back to the same ETag
cannot be distinguished by the condition alone.

## Storage binding

State is bound to the non-secret `DirectStorageBinding` for scheme, storage
profile, bucket, root and endpoint.
The binding stores its 32-byte digest, never credentials. Reusing a state
directory with another bucket, root, profile or endpoint is rejected before
recovery, local installation or remote mutation.

Binding validation rejects URL userinfo, query and fragment components. The
endpoint retains its scheme and case-sensitive path. Provider default endpoint
selection is represented explicitly rather than by an empty or guessed value.

## Durable baseline

The common baseline has one entry per synchronized file path. It records:

- the accepted remote version or ETag, length and observed mtime;
- the accepted local content digest and length;
- the accepted local inode and parent-directory identities.

The local fingerprint is captured from the exact bytes accepted by a successful
conditional response. It is never replaced by a fresh scan after publication.
If a newer local edit exists when an older publication succeeds, the baseline
advances only to the older accepted input and the newer edit remains pending.

Direct has no stable remote file identity. Baseline identity is a storage
observation: a real version ID when both observations expose one, otherwise
ETag, size and known whole-second mtime using the same fallback rule as Direct
reads.

The state database profile is `yinyang-direct-sync-2` with tables `binding`,
`baseline`, `work`, `installs`, `artifacts` and `conflicts`. SQLite uses WAL and
`synchronous=FULL`; every work, install, artifact and completion transition is
durable when committed. An existing database is validated before journal-mode or
schema changes. Initialization rejects an existing database instead of
migrating or reinterpreting it. Schema admission compares exact table and
column-name sets, and older or unknown profile identifiers are rejected with
`Unsupported`.

## Initialization

`sync init --directory ROOT` creates or validates the local root, binds root and
state identities, observes both file sides and creates the empty common
baseline. An unbound namespace admits at most one non-empty file side. If both
sides already contain files, initialization fails and requires explicit
organization rather than inventing a merge order.

Initialization rejects unsupported directory-only namespace state before
creating the database:

- a local directory with no descendant file;
- a remote directory marker with no descendant file.

Direct object storage has no portable empty-directory operation. Synthetic
parent directories required by file keys are supported. An empty local
directory or an empty remote marker is outside the supported namespace and must
be removed or populated. If such a directory appears after binding, `run`
rejects the pass before dispatch.

## Reconciliation

`sync run` performs one finite pass:

1. recover accepted local installations;
2. observe local and remote namespaces and reject unsupported empty
   directories;
3. retry retained work whose original precondition still holds;
4. observe again, scan the local tree and compare baseline, local and remote;
5. execute remote publications, remote deletions, local materializations and
   local deletions in a deterministic order;
6. report pending, Unknown, conflicts and whether the observed state is in sync.

Creates and replacements are applied before deletes so a local or remote rename
can converge as non-atomic destination creation followed by source deletion.
Rename detection is an optimization only. Direct Sync never claims an atomic
remote rename: a pass can end with both names present or with one side
partially applied, and the retained work makes the next retry boundary
explicit.

No watcher or daemon is implied. When removing a file also empties an ancestor
directory that is required by the durable baseline, Direct Sync removes that
now-empty parent with fd-relative `AT_REMOVEDIR` calls and stops at the first
non-empty directory. This keeps a pulled `dir/file` delete convergent and a
later run is a no-op. An unrelated empty directory that is not an ancestor of
accepted or pending file state remains an explicit unsupported namespace entry.

## Remote publication

Before any remote request, Direct Sync:

1. captures the exact local file into a private immutable artifact;
2. records the operation, path, accepted baseline and accepted local
   fingerprint;
3. records the exact ETag or absence precondition;
4. commits the work record as `Prepared`;
5. changes it to `Unknown` and commits it before dispatch.

The state is `Unknown` before the request because a process crash or lost
response cannot prove that storage did not accept it.

Only successful mutation response metadata is `Applied`. A later HEAD or LIST
observation cannot turn an earlier ambiguous request into an accepted commit.
When an `Unknown` request is retried, it reuses the exact retained bytes and
precondition. If the precondition still holds, the retry can produce a
definitive response. If it no longer holds, the original work remains `Unknown`
because the changed object may be the original result or an unrelated external
successor.

A user can explicitly resolve `Unknown` by keeping current local bytes or by
materializing the current observed remote state. These are new, explicit
observed-state decisions. They are not proof that the original ambiguous
request committed.

Remote create uses `if-not-exists`. Remote replace and conditional delete use
the exact baseline ETag. There is no stat-then-unconditional-delete fallback.
The selected backend must provide a real conditional DELETE.

## Local materialization

Pulled bytes are written to a private temporary file under the state directory,
validated against the accepted remote pin, fsynced and recorded in the local
install journal. The journal row and its artifact record are durable
`synchronous=FULL` commits before the local rename, exchange, or deletion that
recovery must interpret.

- Creation uses `RENAME_NOREPLACE`; an occupied local name is a conflict.
- Replacement verifies the scanned inode, parent identity and bytes, then uses
  `RENAME_EXCHANGE`. If the displaced inode changed, the exchange is rolled
  back and the conflicting bytes remain retained.
- Deletion first atomically quarantines the local inode in the state directory
  and verifies its identity and digest before unlinking it. A concurrent
  replacement is restored or retained rather than deleted by name.

The baseline is advanced only after the resulting local bytes or deletion are
accepted and the install journal is terminal. A crash between the local
operation and that transition is recovered by inspecting the recorded target
and temporary identities. Ambiguous local state becomes a conflict; it is not
silently accepted.

## Conflicts and artifacts

Direct Sync preserves both available sides when:

- local and remote both changed after the baseline;
- both sides contain files at a new path;
- a file changed into a directory or a directory into a file;
- a local edit races a remote materialization;
- a prepared conditional remote mutation loses its precondition.

An `Unknown` result remains separately typed from a conflict. Its retained
input is exportable, and replay or explicit resolution is required.

Conflicts have stable identities derived from the path and observable local and
remote evidence. Re-detecting the same evidence refreshes the existing record.
The local side is captured as an immutable artifact when possible. The remote
side is downloaded through its pinned Direct descriptor and retained when
possible.

Conflict resolution is explicit:

- `--keep local` captures current local bytes and publishes them against a
  newly observed current precondition, or conditionally deletes the remote
  object when the local path is absent.
- `--keep remote` materializes the newly observed remote version locally, or
  deletes the local file when the remote path is absent.

`--content PATH` is not accepted because Direct Sync does not define a separate
explicit-content merge contract. A user can edit the local directory, export
both sides and then choose `--keep local`.

An upload or resolution input is released after its work record is terminal and
the accepted baseline is durable. A displaced local inode is released after the
install baseline is durable. Active conflict artifacts stay until an explicit
resolution clears that conflict; Unknown work keeps its exact input. Status
reports the remaining artifact count.

## Status and CLI

The Linux CLI supports:

```text
yy --volume CONFIG sync init --directory ROOT
yy --volume CONFIG sync run
yy --volume CONFIG sync status [--json]
yy --volume CONFIG sync export --artifact ID --output PATH
yy --volume CONFIG sync export --conflict ID --side local|remote --output PATH
yy --volume CONFIG sync resolve ID --keep local|remote
```

`status --json` emits `yinyang.direct-sync.status.v1`. It reports:

- bound/unbound/invalid state and the canonical namespace digest;
- whether the configured binding matches the durable binding;
- remote availability and observation revision;
- local scan classification;
- per-path local, remote and baseline evidence;
- work and local-install states;
- summary `in_sync`, `pending`, `unknown`, `conflicts`, `failed` and retained
  artifact counts.

`in_sync` is true only when the binding is valid, the current local scan is
clean against the baseline, the remote was observed, and no pending, Unknown,
failed or conflicting path remains. A successful `run` does not by itself
prove convergence; `status` is the completion evidence.

`yy --volume CONFIG doctor --json` inspects the Direct Sync SQLite profile,
binding, pending, Unknown, conflict and retained artifact state read-only and
reports it as `sync-state`. Doctor does not acquire the writer lease, initialize
state, run recovery, or establish `summary.in_sync`; use `sync status --json`
for convergence.

`status` and artifact export do not require remote connectivity. They do not
acquire the writer lease or change authoritative state. SQLite read-only open
may create an empty WAL or update derived shared-memory state, but existing
logical rows and accepted bytes remain unchanged. A conflict's local and remote
bytes are retained locally, so both sides can be exported offline.

Exit code `0` means a valid status or completed finite command, `3` means the
state is unbound, `4` means the binding is invalid, and `1` means an argument,
admission or unexpected command failure.

## Limits

- Direct Sync is Linux-only and ordinary-directory only.
- It is finite and explicit; there is no watcher, background worker or
  automatic merge.
- Empty directories, directory modes, ownership, timestamps, ACLs, xattrs,
  symlinks, hard links and special files are unsupported.
- Remote directory markers and synthetic prefixes are not independently
  mutable. Marker deletion or preservation is not claimed when file keys still
  synthesize the directory.
- Replacement changes storage-native identity. Direct Sync does not provide
  Managed stable identity or receipts.
- A multi-page object listing is not an atomic namespace snapshot.
- ETag conditions inherit the selected storage service's collision and ABA
  boundaries.
- Active conflict artifacts grow until explicitly resolved. Ordinary uploads,
  resolution inputs and displaced inodes do not accumulate after their durable
  release events.
