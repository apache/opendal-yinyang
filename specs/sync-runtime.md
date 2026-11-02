# Durable content intents

**Status: experimental in-process content bridge.**

`yinyang::sync::Sync` is the final public content-only coordination API used by
the in-process native frontend. Ordinary-directory reconciliation belongs to
[`sync_engine::SyncEngine`](linux-sync.md); the Managed adapter plans namespace
transactions internally. Sync no longer accepts namespace operations, owns a
FileHandle staging runtime, or acts as a second reconciliation status
authority.

## Public surface

The complete public surface is:

```text
Sync::open(authority, directory) -> Result<Sync>
Sync::validate_restore(directory, filesystem) -> Result<()>
Sync::stage(node, baseline, source) -> Result<EditId>
Sync::publish(edit) -> Result<Revision>
Sync::status(edit) -> Result<ContentStatus>
Sync::inspect(directory) -> Result<SyncInspect>
```

`ContentStatus` reports the edit and frozen CommitId, accepted baseline,
`Prepared`, `Unknown`, `Committed`, `Conflict`, `Retryable` or `Failed` state,
pending/frozen/conflict booleans, receipt or baseline revision, content length,
and an optional durable `Failure`. `SyncInspect` returns those records
read-only. `EditId` is the caller-visible local identity.

The native frontend stages one exact local file, publishes it and polls status.
It does not use FileHandle or Mount staging. A caller that needs a namespace
mutation uses the shared Sync engine rather than this type.

## Staging

`stage` observes the exact supplied baseline revision. It prepares replacement
bytes once from the supplied `AsyncRead`; the resulting `ContentDescriptor` is
the accepted content identity. The local EditId is BLAKE3 with derive-key
context `yinyang-sync-content-edit-2` over filesystem ID, NodeId,
24-byte baseline revision, replacement length and verified content digest.
Repeating the exact intent returns the same row.

The frozen `Transaction` bytes, request digest and deterministic CommitId are
committed with `synchronous=FULL` before any dispatch. There is no separate
FileHandle journal or mutable replacement copy. The frozen transaction restores
the verified prepared descriptor; publication does not read the live local file
again. The authority's bounded preparation cache may avoid a second payload
read, and a cache miss imports and verifies the exact prepared bytes.

The baseline local side is always the accepted input of the request. It is
never taken from the live file at response time. A live file that differs after
completion is pending work or the superseded conflict side, not in-sync input.

## Publication

`publish` is serialized per Sync instance. `Prepared` and `Retryable` requests
restore and dispatch the original transaction with the original CommitId.
Before dispatch, state becomes durable `Unknown`. On a known `Conflict`,
`Retryable` or `Failed` outcome, the corresponding terminal or retry state is
stored with its typed failure.

An `Unknown` request first looks up the original CommitId. A receipt with the
same request digest completes the intent without another dispatch. Absent
receipt does not prove failure, so the exact request may be replayed. A
post-dispatch authority error stays `Unknown` with the CommitId in its durable
failure. No path replans against a newer baseline.

The `intents` state codec is shared by Sync, the Sync engine and
`directory/journal.rs`:

| Code | State |
| --- | --- |
| 0 | Prepared |
| 1 | Unknown |
| 2 | Committed |
| 3 | Conflict |
| 4 | Retryable |
| 5 | Failed |

The exact shared columns are `id`, `request_id`, `request_digest`, `request`,
`commit_id`, `state`, `receipt_revision`, `receipt_ordinal`, `failure` and
`metadata`. Each database supplies its own table definition and binding, but
the state and receipt codec is one implementation. The engine uses `metadata`
for its work association; Directory publication stores its diagnostic source
and manifest there.

## Local profile

The dedicated directory contains only `sync.db` with profile
`yinyang-sync-4`. Its exact table set is `binding` and `intents`; the binding
contains one filesystem identity. SQLite uses WAL with
`synchronous=FULL`. Older, newer or unknown profiles and missing, extra or
malformed tables/columns are rejected without repair or migration.

Validation reads the profile and exact schema before journal-mode changes,
DDL, recovery or dispatch. Read-only inspection may create an empty `-wal` and
update derived `-shm`, but does not change authoritative database bytes,
logical rows or configured journal mode.

Rows remain retained indefinitely in this experimental profile. There is no
automatic garbage collection or conflict-resolution policy. A definitive
`Conflict` or `Failed` intent is not redispatchable; a caller creates a new
intent against suitable predicates.

## Validation

Tests cover conditional content replacement, terminal conflicts, exact
prepared-byte replay after Unknown response loss, receipt attachment after
restart without a second commit, typed status and inspection, and rejection of
an unsupported profile without schema repair.
