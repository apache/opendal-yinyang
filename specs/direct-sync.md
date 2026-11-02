# Linux ordinary-directory Direct Sync

**Status: experimental, Linux-only, finite explicit reconciliation.**

Direct synchronization is one `SyncEngine` profile from
[linux-sync.md](linux-sync.md). It reconciles an ordinary local directory with
one storage-native Direct object namespace. The engine supplies the local scan,
common baseline, reconciliation, conflicts, retained artifacts, local installs,
status and backup. The Direct adapter supplies object observation and exact
ETag-or-absent mutations.

Direct does not create Managed NodeIds, transactions, commit receipts or
authoritative YinYang metadata. It does not provide stable identity across
object replacement.

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

The `staging` field is the Sync state directory. It must be distinct from the
local root, neither path may contain the other, and both must share one
filesystem. State must not be shared with a Direct Mount runtime or another Sync
root.

Admission requires whole-object write, empty-object write, conditional creation
with `if-not-exists`, conditional replacement with exact ETag `If-Match`, and
conditional delete with exact ETag `If-Match`. Missing support is rejected
before state creation or remote mutation. ETag compare-and-swap has the
backend's normal same-ETag/ABA boundary.

Storage binding contains the non-secret scheme, profile, bucket, root and
endpoint digest. It never stores credentials. URL userinfo, query and fragment
components are rejected.

## Capabilities

Direct reports the closed capability set as:

```text
directories = false
executable = false
rename = false
batch = false
max_batch = 1
delta = false
receipt_lookup = false
```

These values select real engine branches. Empty directory markers and executable
metadata are rejected as unsupported. A local or remote file move converges as
create followed by delete. Every remote observation is a full walk. Every
publication is one exact request. An Unknown request is replayed from its frozen
input and original ETag-or-absent precondition; it is never resolved by a later
HEAD or LIST.

## Remote identity

Direct has no stable remote file identity. The accepted remote fingerprint is a
real version ID when both observations expose one, otherwise ETag, size and
known whole-second mtime using the Direct read fallback. The baseline records
that fingerprint and the accepted local content digest and length.

The accepted local side is the exact input accepted by a successful conditional
response. A newer local edit remains pending. Completion of v1 never
acknowledges a live v2.

## Publication and Unknown

Creates use `If-None-Match: *`. Replacement and delete use the exact accepted
ETag `If-Match`. There is no unconditional delete fallback.

The engine captures the exact local input as a retained artifact, records the
frozen request and baseline, and commits `Unknown` before dispatch. Success
metadata is `Applied`; an explicit condition failure is `Conflict`; a transport
loss is `Unknown`; a known not-published validation failure is `Retryable` or
`Failed` according to its typed error.

An Unknown retry reuses the same retained bytes, source identity and
precondition. If the precondition still holds, replay can produce a definitive
response. A user can explicitly choose current local or current remote bytes,
but that creates a new observed-state decision rather than proving the original
request committed.

## Local installation

The engine creates a private fd-relative temporary file and durable `installs`
row. The Direct adapter writes the pinned object into the supplied borrowed fd
and verifies the accepted version/ETag, length and bytes. The engine rechecks
the target inode, parent identity and digest at the effect boundary.

- Creation uses `RENAME_NOREPLACE`; an occupied name is a conflict.
- Replacement uses `RENAME_EXCHANGE`; a changed displaced inode is rolled back
  and retained as a conflict.
- Deletion quarantines the exact local inode before unlinking it. A concurrent
  replacement is restored or retained rather than deleted by name.

Recovery uses only the durable install row. The baseline advances after the
local bytes or deletion and terminal install state are durable.

## Conflicts and resolution

Direct retains both available sides for changed/changed content, create/create,
kind changes, races with materialization, and lost preconditions. Unknown stays
typed separately from Conflict.

`--keep local` captures the current local bytes or absence and applies a new
request against a newly observed remote precondition. `--keep remote`
materializes the newly observed remote version or deletes the local file when
the remote is absent. `--content PATH` is accepted by the shared engine and
freezes the selected bytes against a current remote precondition.

Conflict material remains until explicit resolution. Upload and resolution
inputs are released after the accepted request and baseline are durable.
Displaced local versions survive one reconciliation boundary so late writes
through an old descriptor remain observable.

## Backup

Direct backup and restore use the shared quiesced package format. The manifest
contains `sync-engine.db`, retained artifacts, the local root and checksum
evidence. Restore validates the database and manifest before creating
destinations, maps local inode identities, and preserves intents, accepted
fingerprints, receipts and historical artifact digests.

## Status and CLI

Direct supports the same commands and `yinyang.sync.status.v1` document as
[linux-sync.md](linux-sync.md). `model` is `direct`; the typed intent and
outcome states expose prepared, Unknown, committed, conflict, retryable and
failed work. `status` does not require remote connectivity and does not acquire
the writer lease.

`yy --volume CONFIG doctor --json` inspects the shared state profile read-only
and may report adapter support state. Doctor does not establish
`summary.in_sync`; use `sync status --json` for convergence evidence.

## Limits

- Direct Sync is Linux-only and ordinary-directory only.
- There is no watcher, daemon or automatic merge.
- Empty directories, directory modes, ownership, timestamps, ACLs, xattrs,
  symlinks, hard links and special files are unsupported.
- Remote directory markers and synthetic prefixes are not independently
  mutable.
- Replacement changes storage-native identity.
- A multi-page object listing is not an atomic namespace snapshot.
- An external transition away from and back to the same ETag is not detectable
  by the condition alone.
