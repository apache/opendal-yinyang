# Managed space reporting, retention planning, and logical expiry

**Status: experimental. Reporting and planning are observation-only. Explicit
expiry changes logical revision visibility and never performs physical
deletion.**

The `yy --volume CONFIG space` commands report observed Managed storage and
build an explicit revision-protection plan. A separate explicit command can
make selected historical revisions unavailable to fresh observations while
retaining their history identity, receipts, and physical bytes.

## Commands

```text
yy --volume CONFIG space report [--json]
yy --volume CONFIG space retention plan \
  [--keep-last N] [--keep REVISION]... [--backup PATH]... [--json]
yy --volume CONFIG space retention expire \
  --revision REVISION [--revision REVISION]... [--backup PATH]... [--json]
```

`--volume` is required. Managed `report` and `plan` use the selected authority.
Direct `report` and `plan` keep their existing local-state observation path and
make no Managed authority or namespace-object claim. `expire` is rejected for
Direct or read-only volumes and on non-Linux hosts before authority or
local-state side effects. `expire` requires a writable Managed volume on Linux.
`report` and `plan` remain observation-only even when the volume configuration
is writable.

`space report --json` emits `yinyang.managed-space.report.v5`. `space retention
plan --json` emits `yinyang.managed-retention.plan.v5`. `space retention expire
--json` emits `yinyang.managed-retention.expiry.v1`. Revisions are 24-byte
hexadecimal strings, filesystem identities are 16-byte hexadecimal strings,
and enum values are `snake_case`. Unknown values are `null`; optional evidence
is not represented by fabricated zeroes.

## Admission and read-only effects

Managed `report` and `plan` perform the ordinary strict volume parse and
capability admission, construct the selected authority, and observe existing
state. They do not:

- create a filesystem or call `Fs::create`;
- create, migrate, recover, or clean a local staging database;
- acquire the `runtime.lock` writer lease;
- call `Runtime::open`, `Sync::open`, or a mount entry point;
- publish, repair, acknowledge, or delete namespace or content records; or
- change authoritative existing SQLite/WAL bytes.

`expire` uses the mutation admission and lease rules below.

Appropriate typed history support is authority-specific. The object authority
returns metadata-authenticated readable and expired revision identities from
its history index. The metadata service and authenticated service client
enumerate the durable revision table with bounded pages of at most 256
revisions. Embedded service reads use a read-only SQLite connection; client
pages remain inside the existing 16 MiB RPC frame. The implementation does not
infer history from local staging, current physical objects, or service absence.

Opening a known SQLite database read-only may create an empty WAL file or
create/update derived `-shm` coordination files. Those files are observations,
not a byte-identity requirement. See [File-operation
runtime](file-runtime.md) and [Linux ordinary-directory Managed
Sync](linux-sync.md).

## Space report

The report identifies the filesystem and the exact latest observed revision.
Its `measurement` object states what each number covers.

Logical live content is measured from that one immutable snapshot:

- `files` counts file nodes;
- `directories` includes the root directory;
- `file_bytes` sums logical file lengths, including repeated references;
- `unique_content_count` and `unique_content_bytes` deduplicate equal
  `ContentId` values in that snapshot.

Logical retained content uses the authority's typed revision history. Known
expired identities contribute to `revision_count` and
`expired_revision_count` but are not opened as snapshots. Each successfully
observed readable revision has the same live-content fields.
`readable_revision_count` counts readable identities.
`observed_file_bytes` sums bytes across observed readable snapshots and
therefore counts the same logical content more than once when it appears in
more than one revision. `observed_unique_content_bytes` deduplicates equal
content identities across those observed snapshots. A partial observation is
explicit; unavailable revisions are not treated as empty or expired.

Physical accounting lists current object keys below
`.yinyang/v2/<filesystem>/packs/` and separates the current `.yinyang/head`
object. Each visible pack object key is counted once, regardless of how many
files, revisions, index pages, or extents refer to it. The byte count includes
visible pack objects that may be metadata pages or orphaned preparation output;
it is not a sum of logical file lengths.

The report traverses authenticated physical references reachable from the
observed authority roots:

- object snapshots and retained snapshot descriptors;
- every retained metadata B+ tree page and external value, including nodes,
  directory entries, receipts, ordered changes, and the retained-history index;
- every content hash-index node referenced by a published or historical
  `ContentDescriptor`; and
- content leaf extents as coverage references without downloading their
  payloads.

Only index and metadata pages are read and checksum-verified. Leaf payload
extents are never fetched during reachability, so traversal proves index
reachability and reference coverage, not payload scrub integrity. A missing,
corrupt, foreign, or context-conflicting reference keeps the physical
measurement partial; a partial traversal is never promoted to complete.

`referenced_extents` is therefore
`authenticated_index_references_without_payload_reads`. The report still
cannot calculate reclaimable bytes: `reclaimable_bytes` is `null`.

Physical coverage reports:

- `observed_referenced_bytes`, the union of authenticated extent intervals,
  including references to packs absent from the current-key listing;
- `visible_referenced_bytes`, that union intersected with each visible pack's
  current object length;
- `visible_uncovered_bytes`, every visible pack byte minus
  `visible_referenced_bytes`, including visible packs with zero references;
- `fully_referenced_visible_packs` and
  `partially_referenced_visible_packs`; and
- separate missing and truncated reference-pack counts.

`visible_uncovered_bytes` is not reclaimable. Packs with zero references are
separately reported under `root_external` only when the root set is complete
and the current-key listing is stable. That result is relative to the observed
root set: it is not an orphan claim and never authorizes deletion.

The object inventory is a recursive current-key listing, not an atomic
snapshot. The report observes latest revision before and after inventory. A
changed revision marks the listing as changed during inventory; an unavailable
second observation marks it unverified. Even a stable revision does not turn a
backend listing into a transactional snapshot.

Current-key listing does not reveal noncurrent object versions. The report
always states `noncurrent_versions: "unknown"`; it does not claim a zero total
or an all-versions total. The same unknown applies to external readers,
untracked revision pins, other staging directories, undisclosed backup
packages, and quiescence. Explicitly supplied backup packages are only
observed by `space retention plan --backup`.

Local staging uses only existing paths:

- `<staging>/staging.db` denotes managed mount or file-runtime staging;
- `<staging>/content/staging.db` denotes content staging used by Sync;
- `<staging>/sync.db` denotes the content-only library Sync intent journal;
  and
- `<staging>/sync-engine.db` and the engine-owned artifacts exposed by its
  read-only inspection denote the shared Managed or Direct Sync engine.

No other path is identified as a maintenance component or registered in
`staging.databases`. In particular, a one-shot directory-publication `--state`
journal (`directory.db` and its WAL) is not discovered or registered even when
that state directory is inside the configured staging path. Those frozen
requests remain recoverable through Core receipt lookup or their known base
under the [Directory transfer](directory-transfer.md) contract. Maintenance
expiry does not accept a directory-journal path and creates no registry for
one.

An absent staging database remains absent. A present database is inspected
through the existing read-only runtime inspection API. The report exposes
validity, pending/frozen/conflict/Unknown/failed handle counts, staged handle
lengths, ledger error count, frozen commit identities, and the recorded remote
base revisions. Handle lengths are local staged logical lengths, not directory
allocation.

Every ready durable handle protects its recorded base with `staging_base`,
including a clean handle whose only remaining recovery evidence is its UUID.
Dirty, frozen, conflict, Unknown, and failed state add their corresponding
reasons without replacing the base reason.

When `sync.db` exists, `Sync::inspect` reads its content intents without taking
a writer lease. Intent evidence includes baseline revision, original CommitId,
state (`prepared`, `unknown`, `committed`, `conflict`, `retryable`, or `failed`)
and receipt revision when committed. A pending or Unknown intent protects its
baseline without requiring a staged FileHandle.

When `sync-engine.db` exists, the engine's read-only inspection path exposes the
accepted baseline, typed intents, Unknown requests, conflicts and retained
artifacts for the shipped `yy sync` frontend. Managed baseline pin revisions
are protected with `sync_engine_baseline`; committed intent receipt revisions
use `sync_engine_receipt`; conflict-recorded remote revisions use
`sync_engine_conflict`. Direct engine state is valid evidence with no Managed
revisions to protect.

The JSON document exposes these joins as `staging.sync` and `staging.engine`.
An optional database that does not exist is absent, not malformed; a declared
staging path with no component is `not_initialized`. Present malformed, foreign,
or unknown databases report `unavailable` with their error and are not
repaired. `staging.databases` records each observed database role separately.

Volume-bound `space report` and `space retention plan` call
`inspect_bound_local_staging` with the observed authority filesystem. Existing
read-only binding validators are reused before any local evidence is
attributed:

- `Runtime::validate_staging` validates mount staging;
- `Sync::validate_restore` validates the content-only Sync journal;
  and
- `sync_engine::inspect_state` validates the engine's root, state identity and
  Managed namespace binding.

`staging.binding.state` is `verified` after every present component validates
for the observed authority filesystem. If any component is foreign, malformed,
or cannot be verified, the state is `unavailable`, the binding error is
reported, and all handles, content intents, artifacts, commit identities, and
protected revisions from that staging path are withheld. Foreign work must not
protect revisions for the configured volume.

The standalone library function `inspect_local_staging` remains usable for
offline evidence without an authority. It labels
`staging.binding.state: "unbound"` and must not be interpreted as volume-bound
attribution. Binding validation itself is read-only and does not acquire a
runtime lease, initialize a database, migrate a profile, recover work, or
contact storage.

Directory measurement reports regular-file count and apparent file length. It
does not follow symbolic links and does not claim allocated disk blocks. The
directory walk is non-atomic.

## Retention planning

Retention defaults to `keep_all`. Supplying `--keep-last N` and/or one or more
`--keep REVISION` values selects the bounded policy. `N` must be at least one.
Every plan reports `published_history: "retained_by_default"`. Bounded
selection does not delete, expire, or hide retained revisions, receipts, or
their metadata roots. Known expired identities remain visible with state
`expired`, are not selected, and are never treated as empty history.

An explicit `--backup PATH` package is validated read-only with the existing
backup manifest checksum, shape checks, and package validator. Validation does
not restore, migrate state, acquire a writer lease, or require the original
local root to exist. The package filesystem must match the observed volume;
foreign, corrupt, partial, or unreadable packages are reported and their
revisions are not attributed to the volume.

The selected set is the union of:

- the latest observed revision;
- the newest `N` readable revisions from the authority's typed history, when
  requested;
- every requested readable revision; and
- the base revision recorded by every ready durable staging handle, including
  clean handles that remain recoverable by UUID;
- baselines recorded by content Sync intents; and
- receipt revisions recorded by committed content Sync intents;
- per-entry remote revisions recorded by the Sync engine common baseline and
  committed engine intent receipts; and
- revisions protected by an explicitly supplied backup package: content intent
  baselines, committed receipt revisions, engine conflict revisions, and engine
  baseline revisions.

A requested revision has state `readable`, `expired`, or `not_retained`.
Absent revisions make the plan partial. If the authority cannot list history,
requested-pin membership is `null`, state is `unknown`, and the plan cannot
claim completeness.

Each revision decision records whether it is selected and why. Reasons are
declarative protection evidence, not deletion eligibility. Every revision
outside a bounded selection remains visible with
`outside_bounded_policy`; the plan never treats that reason as permission to
remove data. Expired decisions remain visible with reason `expired`.

Selection reasons are drawn from:

- `current`, `keep_all`, `keep_last`, and `requested_pin`;
- `staging_base`;
- `staging_pending`, `staging_frozen`, `staging_unknown`,
  `staging_conflict`, and `staging_failed`;
- `sync_intent_prepared`, `sync_intent_unknown`, `sync_intent_receipt`,
  `sync_intent_conflict`, `sync_intent_retryable`, and `sync_intent_failed`;
- `sync_receipt`;
- `sync_engine_conflict`, `sync_engine_baseline`, and `sync_engine_receipt`; and
- `backup_staging_base`, `backup_staging_pending`,
  `backup_staging_frozen`, `backup_staging_unknown`,
  `backup_staging_conflict`, `backup_staging_failed`,
  `backup_sync_intent_prepared`, `backup_sync_intent_unknown`,
  `backup_sync_intent_receipt`, `backup_sync_intent_conflict`,
  `backup_sync_intent_retryable`, `backup_sync_intent_failed`,
  `backup_sync_receipt`, `backup_sync_engine_conflict`,
  `backup_sync_engine_baseline`, and `backup_sync_engine_receipt`.

Every ready handle contributes `staging_base` because recovery can still read
an uncovered range from that base and a later write continues to use it for
publication predicates. Dirty, frozen, conflict, Unknown, or failed state adds
the corresponding reason without replacing the base reason. A clean handle is
not disposable protection evidence while its UUID remains in durable staging.

The plan also reports:

- receipt evidence counted from the latest snapshot's retained change records;
- staging pending/frozen/conflict/Unknown/failed evidence;
- content Sync prepared/Unknown/committed/conflict/retryable/failed intent
  evidence;
- engine pending installs, retained artifacts, conflicts and baseline/receipt
  evidence;
- `backups`, with each supplied package's state (`validated`, `partial`,
  `foreign`, or `unavailable`), filesystem binding, read-only staging evidence,
  and protected revisions; and
- frozen commit identities;
- `backup_evidence: "not_supplied"`, `"supplied_validated"`, or
  `"supplied_incomplete"`;
- `external_readers: "unknown"`;
- `untracked_pins: "unknown"`; and
- `quiescence: "not_observed"`.

Receipt records remain revision evidence, not a physical cleanup
authorization. The latest snapshot's total receipt count is descriptive and
does not select every receipt revision. A snapshot is selected by
`sync_receipt` or `backup_sync_receipt` only when an observed local or supplied
backup intent records that exact dependency. Receipt identity, digest, and
outcome remain replayable through Core even when their base snapshot becomes
logically expired.

Backup packages are not discovered automatically. A revision needed by an
undisclosed backup, external reader, or another unpinned workflow must be
supplied as an explicit requested pin or via `--backup`, and quiescence must be
established by the operator outside this command.

`--keep` and `--keep-last` apply only to one invocation; they are not persistent
pins and do not create a registry. Revisions outside a bounded policy remain
readable with `outside_bounded_policy`. Planning never makes a retained
revision unreadable; only the separate explicit `expire` command changes fresh
lookup state.

Every plan has `deletion_authorized: false` and `reclaimable_bytes: null`.
Selecting or excluding a revision does not prove that any extent or object is
unreachable. Shared packs, noncurrent backend versions, prepared orphans,
metadata pages, external readers, and unobserved staging make a physical
cleanup decision a separate future contract.

## Explicit logical expiry

Expiry requires every affected writer, reader, mount, Sync process, and backup
activity for the volume to be stopped. The command's checkable scope is the
configured `staging` path and every explicitly supplied `--backup` package.
Within the configured staging path, only the component evidence listed under
Space report is checkable; a one-shot directory-publication journal under that
path is not discovered or registered. Other staging directories, external
readers, untracked pins, and undisclosed backups remain unknown. The command
reports `quiescence: "not_observed"` and does not claim global isolation.

Admission order is fixed. Direct or read-only volumes, and commands on
non-Linux hosts, are rejected before operator construction, credentials,
authority connection, or local lease acquisition. The static operation check
lives in the volume admission owner and enforces these refusals; the CLI only
invokes it. On Linux, a writable Managed command then acquires the existing
configured writer leases in this order:

1. `sync-engine.lock`;
2. `sync.lock`;
3. `content/runtime.lock`; and
4. the Mount/file runtime `runtime.lock`.

Lease acquisition may create the configured directories and lock files. It
never creates, initializes, migrates, or recovers `sync-engine.db`, `sync.db`,
`content/staging.db`, or `staging.db`. The leases cover evidence inspection and
the dispatch attempt, and are released when the command returns, including
when the authority outcome is `Unknown`. The command does not retain a lease
while an Unknown outcome remains unresolved. After release, keeping every
declared activity stopped is the caller's obligation. A held lease or lease
error refuses mutation.

After acquiring the leases, the command observes and freezes one exact
`expected_latest` revision. It then validates the configured local state and
supplied backups and keeps that revision fixed through dispatch. Core returns
`Conflict` instead of rebasing if the authority changed.

An optional component database that is absent is declared absent evidence, not
an empty set inferred from a failed read. A present component that is corrupt,
foreign, unreadable, or otherwise unverifiable refuses mutation. Every
explicitly supplied backup must validate completely; missing, corrupt,
foreign, partial, or unavailable supplied packages also refuse mutation.

The protected set is the union of:

- the fixed expected latest revision;
- every ready durable handle base, including clean handles recoverable only by
  UUID;
- every content Sync intent baseline and committed receipt revision;
- every Sync engine baseline entry revision, committed intent receipt, and
  conflict-recorded remote revision; and
- the same dependencies read from each validated supplied backup.

The latest snapshot's generic receipt count is not a protection set. A
committed receipt's identity, digest, and outcome are preserved by Core
independently of whether its base snapshot remains readable.

A protected revision that is missing, already expired, or cannot be verified
blocks the operation. The maintenance command owns this gate; Core receives
only the bounded eligible candidate set. No remote or local sidecar registry is
created.

Candidate classification reads only the deduplicated union of requested
candidates, protected revisions, and the expected latest revision. The current
latest snapshot is reused instead of fetched again. A fresh typed observation
classifies each other identity as `readable`, `expired`, or missing; any other
error is `unavailable` and refuses mutation. Expiry does not scan all history
merely to expire a small candidate set.

Candidate decisions are:

- `eligible`: readable, retained, and outside the protected set;
- `already_expired`: known expired and outside the protected set;
- `current`, `missing`, or `unavailable`: invalid candidate state;
- `protected`, `protected_missing`, or `protected_expired`: unsafe dependency.

The input is deduplicated and bounded to Core's 256-candidate request limit.
Any current, missing, unavailable, protected, incomplete-evidence, or
unverifiable candidate refuses the whole invocation before mutation. If all
requested candidates are confirmed already expired after every local and
backup check succeeds, the command reports `already_expired` with exit code
zero and issues no new mutation. If expired and eligible candidates are mixed,
only the eligible identities are sent atomically.

Each invocation generates a new operation identity and reports it as audit
evidence. The public CLI has no operation-id option and creates no local retry
journal. The authority request binds the volume, operation identity, exact
`expected_latest`, and canonical candidate set. The report also contains a
local `evidence_digest` over the observed maintenance evidence; it is audit
evidence, not a Core request field or a global completeness claim.

Exit and recovery behavior is:

- `0`: `committed`, or all desired candidates were already expired;
- `2`: `refused` or expected-latest `conflict`; no new expiry was published;
- `3`: `retryable` or `unknown`; the request may still take effect.

`conflict`, `refused`, `retryable`, and `unknown` emit the structured expiry
document. Command, admission, lease, unreadable-history, or internal failures
that prevent a trustworthy document return a non-zero ordinary error without
inventing a refusal document.

After `retryable`, `unknown`, or a process crash, the command no longer holds
its leases, and keeping every declared local and backup activity stopped is the
caller's obligation. Rerun the same explicit candidates and backups under the
leases and verify the monotonic desired candidate state. Absence of a marker
on one read is not proof that an earlier delayed request failed, and the CLI
does not claim to replay or find the original expiry receipt. Resume only
after the desired revisions are confirmed expired, or a new operation has
committed. Core retains exact `ExpiryRequest` byte replay for lower-level
callers that store that request; that is separate from the CLI's
same-candidate recovery.

Fresh observation of a known expired revision returns `Expired`; a revision
that never existed returns `NotFound`. Already-held immutable snapshots and
file versions remain usable. Default keep-all and ordinary publication do not
resurrect an expired revision. Original frozen requests, CommitIds, request
digests, and committed receipt lookup remain replayable even when their base
revision is expired.

Every expiry report states `deletion_authorized: false`,
`reclaimable_bytes: null`, and `physical_cleanup: "not_performed"`. Expiry may
add metadata and advance mutable authority state, but it does not delete,
rewrite, compact, or truncate pre-existing immutable content or packs.

## Report coverage

The JSON documents preserve these distinctions:

- `logical.live` is one latest immutable observation;
- `logical.retained` is authority revision metadata and may be partial or
  unavailable;
- `physical` contains a non-atomic current-object listing and the authenticated
  reference/coverage traversal;
- `staging` is read-only local evidence from configured existing paths;
- `unresolved` identifies evidence that could not be observed; and
- `limits` records accounting and authorization boundaries.

The human-readable forms use the same observations and state unavailable or
unknown evidence explicitly. Successful document production returns exit code
zero even when optional measurements are partial; malformed configuration,
admission failure, or inability to observe the Managed authority returns a
non-zero error without an initialization fallback.

## Example

```shell
yy --volume notes.json space report --json
yy --volume notes.json space retention plan --keep-last 8 --json
yy --volume notes.json space retention plan \
  --keep-last 2 \
  --keep 00112233445566778899aabbccddeeff0011223344556677 \
  --json
yy --volume notes.json space retention plan \
  --backup /backups/notes-2026-11-01 \
  --json
yy --volume notes.json space retention expire \
  --revision 00112233445566778899aabbccddeeff0011223344556677 \
  --backup /backups/notes-2026-11-01 \
  --json
```

## Limits

This contract does not provide physical garbage collection, deletion,
compaction, repacking, a persistent remote pin registry, background scheduling,
default expiry, or a release-supported storage reclamation promise. `--keep`
is a one-invocation declaration, not a persistent pin. Explicit backup
packages are validated read-only and do not require the original local root;
they remain protection evidence, not deletion authorization. A revision
needed by an undisclosed backup, external reader, or unpinned workflow must
still be supplied explicitly. It does not inspect Direct namespaces. It does
not make SQLite sidecar files byte-identical or infer noncurrent backend
versions that the configured backend does not return. Reachability does not
scrub content payloads, and a complete observed root set does not prove global
orphanhood, external-reader absence, or deletion eligibility.
