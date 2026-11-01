# Managed space reporting, retention planning, and logical expiry

**Status: experimental. Reporting and planning are non-destructive; expiry is
an explicit logical metadata mutation and never performs physical deletion.**

The `yy --volume CONFIG space` commands report observed Managed storage and
build an explicit revision-protection plan. The explicit expiry command makes
selected historical revisions unreadable while retaining their identity and
marker. These commands do not delete content, run garbage collection, create a
remote pin protocol, or estimate reclaimable bytes.

## Commands

```text
yy --volume CONFIG space report [--json]
yy --volume CONFIG space retention plan \
  [--keep-last N] [--keep REVISION]... [--backup PATH]... [--json]
yy --volume CONFIG space retention expire \
  --revision REVISION [--revision REVISION]... \
  [--backup PATH]... [--json]
```

`--volume` is required. The configuration must use `model: managed`; Direct
volumes and their namespace objects are rejected before authority connection.
`space report` and `space retention plan` are observation-only even when the
volume configuration is writable.

`space report --json` emits `yinyang.managed-space.report.v3`. `space retention
plan --json` emits `yinyang.managed-retention.plan.v3`. `space retention expire
--json` emits `yinyang.managed-retention.expiry.v1`. Revisions are 24-byte
hexadecimal strings, filesystem identities are 16-byte hexadecimal strings,
and enum values are `snake_case`. Unknown values are `null`; optional evidence
is not represented by fabricated zeroes.

## Admission and read-only effects

`space report` and `space retention plan` perform the ordinary strict volume
parse and capability admission, construct the selected Managed authority, and
observe existing state. They do not:

- create a filesystem or call `Fs::create`;
- create, migrate, recover, or clean a local staging database;
- acquire the `runtime.lock` writer lease;
- call `Runtime::open`, `Sync::open`, or a mount entry point;
- publish, repair, acknowledge, or delete namespace or content records; or
- change authoritative existing SQLite/WAL bytes.

`space retention expire` uses the separate admission and mutation rules below.

Appropriate `retained_revisions` support is authority-specific. The object
authority returns metadata-authenticated revisions from its existing history
index. The metadata service and authenticated service client enumerate the
durable revision table with bounded pages of at most 256 revisions. Embedded
service reads use a read-only SQLite connection; client pages remain inside the
existing 16 MiB RPC frame. The implementation does not infer history from local
staging, current physical objects, or service absence.

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

Logical retained content uses the authority's retained revision index. Each
successfully observed revision has the same live-content fields.
`observed_file_bytes` sums bytes across observed snapshots and therefore counts
the same logical content more than once when it appears in more than one
revision. `observed_unique_content_bytes` deduplicates equal content identities
across those observed snapshots. A partial observation is explicit; unavailable
revisions are not treated as empty.

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
- `<staging>/sync.db` denotes the Managed Sync namespace and content journal;
- `<staging>/content/staging.db` denotes Managed Sync content staging; and
- `<staging>/replica.db` denotes the Linux ordinary-directory replica.

An absent staging database remains absent. A present database is inspected
through the existing read-only runtime inspection API. The report exposes
validity, pending/frozen/conflict/Unknown/failed handle counts, staged handle
lengths, ledger error count, frozen commit identities, and the recorded remote
base revisions. Handle lengths are local staged logical lengths, not directory
allocation.

When `sync.db` exists, the existing `Sync::inspect` API joins content-edit
records and namespace intents without taking the writer lease. Namespace
evidence includes baseline revision, original CommitId, operation state
(`prepared`, `unknown`, `committed`, `conflict`, `retryable`, or `failed`), and
receipt revision when committed. A namespace-only pending or Unknown intent can
therefore protect its baseline even when no content handle exists. Content
handles already counted from content staging are not counted again from this
join.

When `replica.db` exists on Linux, the existing read-only `Replica::inspect`
API reports incomplete installs, retained artifacts, exportability, conflicts,
and the stored common baseline. The baseline evidence contains the global
baseline revision plus the distinct per-entry remote revisions used as future
staging predicates. Conflict-recorded and baseline-recorded remote revisions
are protected with `replica_conflict` and `replica_baseline`. `baseline_coverage`
is `observed_from_read_only_baseline` when the baseline is readable. A failure
to open the bound local root marks replica evidence unavailable rather than
fabricating zero.

The JSON document exposes these joins as `staging.sync` and `staging.replica`.
Missing files report `missing`; malformed or unknown existing databases report
`unavailable` with their error and are not repaired. `staging.databases` records
each observed database role separately.

Volume-bound `space report` and `space retention plan` call
`inspect_bound_local_staging` with the observed authority filesystem. Existing
read-only binding validators are reused before any local evidence is
attributed:

- `Runtime::validate_staging` validates mount staging and standalone Sync
  content staging;
- `Sync::validate_restore` validates the Sync journal and its content staging;
  and
- `Replica::inspect` supplies the replica's bound filesystem.

`staging.binding.state` is `verified` after every present component validates
for the observed authority filesystem. If any component is foreign, malformed,
or cannot be verified, the state is `unavailable`, the binding error is
reported, and all handles, namespace intents, artifacts, commit identities, and
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
their metadata roots.

An explicit `--backup PATH` package is validated read-only with the existing
backup manifest checksum, shape checks, and package validator. Validation does
not restore, migrate state, acquire a writer lease, or require the original
local root to exist. The package filesystem must match the observed volume;
foreign, corrupt, partial, or unreadable packages are reported and their
revisions are not attributed to the volume.

The selected set is the union of:

- the latest observed revision;
- the newest `N` revisions from the authority's retained-revision list, when
  requested;
- every requested revision that is present in the retained list; and
- the base revision recorded by every ready durable staging handle, including
  clean handles that remain recoverable by UUID;
- baselines recorded by Managed Sync namespace intents, including
  namespace-only intents without a content handle; and
- receipt revisions recorded by committed Managed Sync namespace intents;
- global and per-entry remote revisions recorded by the local replica common
  baseline; and
- revisions protected by an explicitly supplied backup package: content
  staging remote bases, namespace intent baselines, committed receipt
  revisions, replica conflict revisions, and replica baseline revisions.

A requested revision absent from a successfully observed retained list is
reported as `not_retained` and makes the plan partial. If the authority cannot
list retained revisions, requested-pin membership is `null`, not `false`, and
the plan cannot claim completeness.

Each revision decision records whether it is selected and why. Reasons are
declarative protection evidence, not deletion eligibility. Every revision
outside a bounded selection remains visible with
`outside_bounded_policy`; the plan never treats that reason as permission to
remove data.

Selection reasons are drawn from:

- `current`, `keep_all`, `keep_last`, and `requested_pin`;
- `staging_base`;
- `staging_pending`, `staging_frozen`, `staging_unknown`,
  `staging_conflict`, and `staging_failed`;
- `sync_namespace_pending`, `sync_namespace_unknown`,
  `sync_namespace_receipt`, `sync_namespace_conflict`,
  `sync_namespace_retryable`, and `sync_namespace_failed`;
- `sync_receipt`; and
- `replica_conflict` and `replica_baseline`; and
- `backup_staging_base`, `backup_staging_pending`,
  `backup_staging_frozen`, `backup_staging_unknown`,
  `backup_staging_conflict`, `backup_staging_failed`,
  `backup_sync_namespace_pending`, `backup_sync_namespace_unknown`,
  `backup_sync_namespace_receipt`, `backup_sync_namespace_conflict`,
  `backup_sync_namespace_retryable`, `backup_sync_namespace_failed`,
  `backup_sync_receipt`, `backup_replica_conflict`, and
  `backup_replica_baseline`.

Every ready handle contributes `staging_base` because recovery can still read
an uncovered range from that base and a later write continues to use it for
publication predicates. Dirty, frozen, conflict, Unknown, or failed state adds
the corresponding reason without replacing the base reason. A clean handle is
not disposable protection evidence while its UUID remains in durable staging.

The plan also reports:

- receipt evidence counted from the latest snapshot's retained change records;
- staging pending/frozen/conflict/Unknown/failed evidence;
- Managed Sync prepared/Unknown/committed/conflict/retryable/failed namespace
  evidence;
- replica pending installs, retained artifacts, conflicts, and global/per-entry
  baseline evidence;
- `backups`, with each supplied package's state (`validated`, `partial`,
  `foreign`, or `unavailable`), filesystem binding, read-only staging evidence,
  and protected revisions; and
- frozen commit identities;
- `backup_evidence: "not_supplied"`, `"supplied_validated"`, or
  `"supplied_incomplete"`;
- `external_readers: "unknown"`;
- `untracked_pins: "unknown"`; and
- `quiescence: "not_observed"`.

The generic receipt count is descriptive evidence, not a blanket revision
pin. A snapshot is selected for `sync_receipt` or `backup_sync_receipt` only
when an observed local or supplied backup namespace intent records that
dependency. A receipt-only historical snapshot is not selected merely because
the latest snapshot contains a receipt for it.

Receipt records remain namespace evidence, not a physical cleanup
authorization. Backup packages are not discovered automatically. A revision
needed by an undisclosed backup, external reader, or another unpinned workflow
must be supplied as an explicit requested pin or via `--backup`, and quiescence
must be established by the operator outside this command.

`--keep` and `--keep-last` apply only to one invocation; they are not persistent
pins and do not create a registry. Revisions outside a bounded policy remain
readable with `outside_bounded_policy`. Planning, including a bounded selection,
never makes a retained revision unreadable. Only the separate explicit expiry
command can change a revision's readable state.

Every plan has `deletion_authorized: false` and `reclaimable_bytes: null`.
Selecting or excluding a revision does not prove that any extent or object is
unreachable. Shared packs, noncurrent backend versions, prepared orphans,
metadata pages, external readers, and unobserved staging make a physical
cleanup decision a separate future contract.

## Explicit logical expiry

Expiry requires every affected writer, reader, mount, Sync process, and backup
activity for the volume to be stopped. A metadata authority daemon may remain
online and process the maintenance RPC. The CLI does not infer quiescence from
a successful command and reports `quiescence: "not_observed"`.

For the configured local staging path, expiry acquires the existing protocol
leases in the same order as the real writers:

1. `replica.lock`;
2. `content/runtime.lock`; and
3. the Mount/file runtime `runtime.lock`.

Acquisition may create the configured directory, `content/`, and those derived
lock files, but it never creates, migrates, or recovers `staging.db`, `sync.db`,
`content/staging.db`, or `replica.db`. The guards remain held through the Core
mutation. A held writer lease, malformed or foreign state, partial evidence, or
unavailable state refuses the operation before mutation. SQLite `-wal`/`-shm`
files and derived locks are not authoritative database bytes.

These leases protect the configured local state only for the lifetime of the
running command. They are not a durable or global fence across a crash,
another unconfigured staging path, external readers, or undisclosed backups.

Every ready durable handle protects its base revision as `staging_base`,
including clean handles recoverable only by UUID. Dirty/frozen/conflict/
Unknown/failed reasons remain additive. Validated backup packages use
`backup_staging_base` and their other backup-specific reasons. A stale
protected revision that is missing or already expired is refused rather than
silently dropped.

The requested candidates are classified against the detailed authority
history:

- `eligible`: retained and unprotected, and can be expired;
- `already_expired`: target state is confirmed and no mutation is needed;
- `current`, `missing`, `protected`, `protected_missing`, or
  `protected_expired`: refuse the whole request.

The input set is deduplicated. If all targets are already expired, the command
returns success without issuing a new mutation. If already-expired and eligible
targets are mixed, only eligible targets are sent as candidates. Any
current/missing/protected/incomplete input blocks every new mutation. Expiry
does not reinterpret this as replay of the original request receipt.

The CLI generates a new operation identity for every invocation and reports it
as audit evidence. It creates no local retry journal and exposes no
user-supplied replay identity. The authority request binds the volume,
operation identity, expected latest revision, canonical candidate/protected
sets, and an opaque canonical evidence digest. Core validates existence,
current revision, protection, evidence outcomes, and expected latest before
writing under its existing atomic publication boundary.

If the command returns retryable/unknown or its process crashes, keep every
affected client and backup activity stopped. Rerun the same candidate set and
confirm it reaches `committed` or the desired `already_expired` state before
resuming them. A read that does not yet observe an expiry marker is not proof
that a delayed earlier request can no longer take effect. Core retains its
lower-level exact `ExpiryRequest` replay semantics, but the CLI does not claim
that rerunning equivalent flags or reusing an identity replays the original
canonical request.

Exit codes are:

- `0`: committed, or every requested target was already expired;
- `2`: refused or expected-latest conflict, with no new expiry;
- `3`: retryable or unknown outcome; keep clients and backups stopped and rerun
  the same candidate set to confirm the target state.

An expired revision retains its identity. `observe_revision` returns
`ErrorKind::Expired` rather than `NotFound`, and detailed history exposes the
stored expiry marker. Default keep-all and ordinary commits do not resurrect
an expired revision. Receipt identity, canonical request replay, and receipt
metadata remain separate from snapshot readability.

Expiry may add metadata and advance mutable `.yinyang/head` or metadata-service
SQL rows. It must not delete, rewrite, or truncate pre-existing immutable
content/packs. The result always states `physical_cleanup: "not_performed"`,
`deletion_authorized: false`, and `reclaimable_bytes: null`.

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
default expiry, or a release-supported storage reclamation promise. `--keep` is
a one-invocation declaration, not a persistent pin. Explicit backup packages
are validated read-only and do not require the original local root; they remain
protection evidence, not deletion authorization. A revision needed by an
undisclosed backup, external reader, or unpinned workflow must still be
supplied explicitly. Expiry cannot prove those omitted activities absent. It
does not inspect Direct namespaces. It does not make SQLite sidecar files
byte-identical or infer noncurrent backend versions that the configured backend
does not return. Reachability does not scrub content payloads, and a complete
observed root set does not prove global orphanhood, external-reader absence, or
deletion eligibility.
