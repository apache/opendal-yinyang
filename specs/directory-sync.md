# Linux ordinary-directory Managed Sync

**Status: experimental finite reconciliation on top of the durable Sync intents.**

`yinyang::replica::DirectorySync` reconciles one local ordinary directory with
one Managed remote root. It owns the common baseline, local installation and
conflict records; durable conditional publication belongs to
[`yinyang::sync::Sync`](sync-runtime.md). It is Linux-only and performs explicit
finite reconciliation runs; no watcher or daemon is implied.

## Common baseline

The baseline records the last agreed state, one entry per synchronized path
(excluding the root, which is the binding anchor):

- the accepted remote `NodeId`, kind, executable bit and content digest;
- the accepted remote revision for that entry, used as the staging predicate
  and as status evidence, never a freshly observed latest revision;
- the accepted local fingerprint (kind, length, content digest, executable bit)
  together with its inode and parent identity, captured when the entry was
  accepted.

The baseline exists only after admission and is stored in the replica database.
Status never reports `in_sync` for a binding without an accepted common
baseline.

## Admission

An unbound state directory admits exactly one empty side. Recreation validates
this before writing any baseline, then persists a root-only common baseline and
hands the run to the same per-entry reconciler used by every later run. Two
non-empty sides are an explicit conflict, not an implicit merge.

## Durable work association

Before a staged edit is dispatched, the directory layer persists an association
keyed by the edit identity: the current path, original and desired path, the
operation, the accepted local fingerprint and identity, the baseline node and
revision, any known original commit identity, the linked conflict, and the
retained input artifact. Retry publishes only associated work and never
resurrects orphaned staging.

Receipt application is ordered so an interrupted run is recoverable: the common
baseline (and any linked-conflict clearance) is applied from the returned
revision before the association is marked complete. Publication is idempotent,
so re-running the exact request after a crash resolves to the same commit.

## Reconciliation

Each run scans the local tree, observes the remote, and performs a three-way
comparison of baseline, local and remote per entry, ordering parents before
children for creation and children before parents for removal. Renames are
detected by identity and applied as one per-root move with each descendant's
accepted fingerprint relocated, so a namespace receipt never acknowledges a
concurrent content edit.

A namespace operation stages against the path's own accepted revision.
Only paths whose parent directory entry list this run already changed advance to
the running revision, so a child removal permits its parent's later removal
without rebasing unrelated paths onto a newer global revision and hiding an
external edit.

A remote deletion of a directory is quarantined as one unit; when any local
descendant diverges from the common baseline the directory is retained and the
unchanged retained entries are recorded as explicit conflicts instead of being
removed.

## Conflicts and resolution

A conflict has a stable 32-hex identity derived from stable evidence (paths,
observable sides, baseline and remote node). Detecting the same unresolved
evidence again refreshes that record rather than inserting a duplicate, and
reuses the first retained backup. A side is classified by presence
(`current_file`, `current_directory`, `deleted`, `retained_artifact`,
`unknown`). `retained_artifact` is reserved for a version that exists only in
retained state, such as a displaced inode written after an atomic exchange; an
ordinary content divergence is `current_file` on both sides even when a backup
copy is preserved separately.

Resolution is durable and choice-specific. `KeepLocal` and explicit `Content`
retain the chosen input as an artifact and stage a durable conditional request
against the recorded predicate; the conflict clears only after the receipt
commits, and a stale predicate produces a new conflict rather than overwriting
the remote. When the chosen state already matches, that request may be a no-op
and the conflict clears without a remote mutation. `KeepRemote` issues no new
remote request: it materializes or quarantines the local path from the recorded
remote evidence (the recorded node version or deletion), applies the resulting
baseline, and clears the conflict once the local result is durable.

## Read-only inspection

`DirectorySync::inspect` (and `inspect_local` when no authority can be
constructed) reports the binding, a full local scan classified against the
baseline, live remote availability, per-entry local/remote/baseline evidence,
per-entry intent state (including a preserved original commit identity) and
outcome, active conflicts and retained artifacts. It acquires no writer lease,
runs no recovery and performs no writes, so it remains usable while a writer
runs and while the remote is unreachable. The adapter owns the versioned JSON
projection; the library exposes only domain evidence.

## Export

Retained artifacts export their current bytes, including writes that landed
after the exchange. A conflict side exports its retained artifact or live local
bytes (`local`) or the recorded remote node version (`remote`). Directory
artifacts expose their retained path; byte-stream export of a directory is
unsupported.

## Backup and isolated restore

`DirectorySync::backup` captures a quiesced package containing the local root,
`replica.db`, `sync.db`, content staging, retained artifacts, and a
checksum-protected manifest. `DirectorySync::restore` validates the package
before creating destination trees, copies it to absent root/state paths, and
maps the original root/quarantine identities to their copied identities. Work
records, accepted fingerprints, CommitIds, and historical artifact digests
remain unchanged. Incomplete installs are rejected rather than reinterpreted.
See [Linux backup and isolated recovery](../docs/linux-backup-recovery.md).
