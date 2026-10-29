# Managed space reporting and retention planning

**Status: experimental and non-destructive.**

The `yy --volume CONFIG space` commands report observed Managed storage and
build an explicit revision-protection plan. They do not delete data, run
garbage collection, create a remote pin protocol, or estimate reclaimable
bytes.

## Commands

```text
yy --volume CONFIG space report [--json]
yy --volume CONFIG space retention plan [--keep-last N] [--keep REVISION]... [--json]
```

`--volume` is required. The configuration must use `model: managed`; Direct
volumes and their namespace objects are rejected before authority connection.
The command is observation-only even when the volume configuration is writable.

`space report --json` emits `yinyang.managed-space.report.v1`. `space retention
plan --json` emits `yinyang.managed-retention.plan.v1`. Revisions are 24-byte
hexadecimal strings, filesystem identities are 16-byte hexadecimal strings,
and enum values are `snake_case`. Unknown values are `null`; optional evidence
is not represented by fabricated zeroes.

## Admission and read-only effects

The command performs the ordinary strict volume parse and capability admission,
constructs the selected Managed authority, and observes existing state. It
does not:

- create a filesystem or call `Fs::create`;
- create, migrate, recover, or clean a local staging database;
- acquire the `runtime.lock` writer lease;
- call `Runtime::open`, `Sync::open`, or a mount entry point;
- publish, repair, acknowledge, or delete namespace or content records; or
- change authoritative existing SQLite/WAL bytes.

Appropriate `retained_revisions` support is authority-specific. The object
authority returns metadata-authenticated revisions from its existing history
index. The built-in metadata service and service client do not expose a
retained-revision enumeration operation, so they report retained-revision
discovery as unavailable. The implementation does not infer history from local
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

This implementation does not publicly enumerate every physical extent
referenced by a snapshot. Therefore it cannot attribute pack bytes to a
revision, prove that a pack is unreferenced, or calculate reclaimable bytes.
`referenced_extents` is reported as unavailable and `reclaimable_bytes` is
`null`.

The object inventory is a recursive current-key listing, not an atomic
snapshot. The report observes latest revision before and after inventory. A
changed revision marks the listing as changed during inventory; an unavailable
second observation marks it unverified. Even a stable revision does not turn a
backend listing into a transactional snapshot.

Current-key listing does not reveal noncurrent object versions. The report
always states `noncurrent_versions: "unknown"`; it does not claim a zero total
or an all-versions total. The same unknown applies to external readers,
untracked revision pins, other staging directories, backup packages, and
quiescence.

Local staging uses only existing paths:

- `<staging>/staging.db` denotes managed mount or file-runtime staging;
- `<staging>/content/staging.db` denotes Managed Sync content staging.

An absent staging database remains absent. A present database is inspected
through the existing read-only runtime inspection API. The report exposes
validity, pending/frozen/conflict/Unknown/failed handle counts, staged handle
lengths, ledger error count, frozen commit identities, and the recorded remote
base revisions. Handle lengths are local staged logical lengths, not directory
allocation.

Directory measurement reports regular-file count and apparent file length. It
does not follow symbolic links and does not claim allocated disk blocks. The
directory walk is non-atomic.

## Retention planning

Retention defaults to `keep_all`. Supplying `--keep-last N` and/or one or more
`--keep REVISION` values selects the bounded policy. `N` must be at least one.
The selected set is the union of:

- the latest observed revision;
- the newest `N` revisions from the authority's retained-revision list, when
  requested;
- every requested revision that is present in the retained list; and
- remote base revisions recorded by existing pending, frozen, conflict,
  Unknown, or failed staging handles.

A requested revision absent from a successfully observed retained list is
reported as `not_retained` and makes the plan partial. If the authority cannot
list retained revisions, requested-pin membership is `null`, not `false`, and
the plan cannot claim completeness.

Each revision decision records whether it is selected and why. Reasons are
declarative protection evidence, not deletion eligibility. Every revision
outside a bounded selection remains visible with
`outside_bounded_policy`; the plan never treats that reason as permission to
remove data.

The plan also reports:

- receipt evidence counted from the latest snapshot's retained change records;
- staging pending/frozen/conflict/Unknown/failed evidence;
- frozen commit identities;
- `backup_evidence: "not_observed"`;
- `external_readers: "unknown"`;
- `untracked_pins: "unknown"`; and
- `quiescence: "not_observed"`.

Receipt records remain namespace evidence, not a physical cleanup
authorization. Backup packages are not discovered automatically. A revision
needed by a backup, external reader, or another unpinned workflow must be
supplied as an explicit requested pin, and quiescence must be established by
the operator outside this command.

Every plan has `deletion_authorized: false` and `reclaimable_bytes: null`.
Selecting or excluding a revision does not prove that any extent or object is
unreachable. Shared packs, noncurrent backend versions, prepared orphans,
metadata pages, external readers, and unobserved staging make a physical
cleanup decision a separate future contract.

## Report coverage

The JSON documents preserve these distinctions:

- `logical.live` is one latest immutable observation;
- `logical.retained` is authority revision metadata and may be partial or
  unavailable;
- `physical` is a non-atomic current-object listing;
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
```

## Limits

This contract does not provide physical garbage collection, deletion,
compaction, repacking, a persistent remote pin registry, background scheduling,
or a release-supported storage reclamation promise. It does not inspect Direct
namespaces. It does not make SQLite sidecar files byte-identical or infer
noncurrent backend versions that the configured backend does not return.
