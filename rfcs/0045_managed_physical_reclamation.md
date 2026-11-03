- Proposal Name: `managed_physical_reclamation`
- Start Date: 2026-11-03
- RFC PR: unsubmitted
- Tracking Issue: [apache/opendal-yinyang#78](https://github.com/apache/opendal-yinyang/issues/78)

# Summary

Define explicit, quiesced physical reclamation for Managed volumes. The first
useful scope releases whole packs that are reachable only through expired,
unprotected history. It also removes packs left by preparation that never
published, but that cleanup alone is not sufficient for this contract.

Reclamation uses one atomic retention cut followed by an idempotent whole-pack
sweep. The cut preserves revision identity and advances the authority; the
sweep changes no logical state. Live-data compaction, automatic scheduling, and
a persistent reclamation registry are outside the first contract.

# Motivation

Logical expiry currently changes fresh revision lookup while retaining every
physical byte. The authenticated reachability traversal treats expired snapshot
descriptors and content descriptors in change records as physical roots.
Current-key coverage can show that a pack has unreferenced bytes, but neither
logical expiry nor those uncovered bytes proves that deleting the pack is safe.

A physical contract must instead answer:

- which authority metadata and content references keep a pack alive;
- whether expired-history-exclusive data can be released without erasing the
  revision identity or receipt history;
- when a held view, frozen request, local state, or supplied backup still needs
  bytes from an expired revision;
- how a crash or unknown response can be retried without a durable job; and
- which storage bytes and backend versions are actually in scope.

Without those answers, deleting the first pack with no observed byte coverage
would turn an observation into an unsafe assertion of orphanhood.

Evidence status: the format, root, and replay statements below are derived from
the current Core implementation and specifications. No reclamation
implementation, measurement, or backend versioning qualification exists.
Noncurrent backend behavior remains unknown and outside the claimed result.

# Non-goals

- No deletion command, automatic expiry, background scheduler, or default
  reclamation policy.
- No new public revision status or pin registry.
- No live-data compaction, range deletion, or reference relocation.
- No version-specific backend deletion or all-versions accounting.
- No claim of global orphanhood outside the declared stopped-process and backup
  scope.
- No implementation or persistent format change in this RFC.

# Proposed Design

## Reclamation unit and first useful scope

The unit of physical deletion is one current-key object at
`.yinyang/v2/<filesystem>/packs/<object-uuid>`. A pack is deleted in full or
retained in full. Partially covered bytes inside a retained pack are not
reclamation candidates.

Expiry is a prerequisite, not authorization. Reclamation selects only known
revisions that are already `Expired`, are outside the declared protection set,
and are explicitly named by the operation. A readable revision, an expired
protected revision, a revision selected only by a read-only retention plan, or
a revision outside the candidate set is not eligible.

The first useful contract must release at least one content pack whose only
authenticated references came from such an expired revision. Removing packs
that no authority metadata references is also allowed, but an implementation
that can only remove those preparation orphans has not implemented this scope.

## Stopped-process and declared evidence boundary

Reclamation requires every writer, reader, mount, Sync process, and client for
the filesystem to be stopped. For a metadata-service authority, the service
process must also be stopped; maintenance opens the existing database profile
under its existing admission rules instead of relying on a live service
transaction. This avoids a transaction that prepared content before the cut
from publishing after the sweep.

For an object authority, the atomic cut fences a writer that already observed an
older head because its conditional replacement no longer matches. It does not
invalidate prepared content held by a process that is still running. Stopping
the process is therefore part of the safety contract, not an operational hint.

Held `Snapshot` and `FileVersion` values are process-local reads of the retained
authority. The operation cannot observe them. The cut does not alter their
metadata, but the later sweep can remove bytes they still intend to read. Any
such view is outside the declared stopped-process window.

The checkable local scope reuses the existing Managed retention evidence:

- the configured staging databases and their validated filesystem binding;
- ready durable handle bases, including clean handles recoverable only by UUID;
- Sync baselines, intent receipts, conflicts, and incomplete installs; and
- every explicitly supplied backup package, validated completely.

Any present component that is corrupt, foreign, unreadable, partial, or not
bound to the observed filesystem refuses reclamation. Any durable request or
install that is not `Committed` refuses reclamation. Such state can still hold
an exact base or prepared content that is not referenced by published remote
metadata. Refusing the whole operation avoids adding a new parser for prepared
content roots.

Committed receipts remain lookupable, but a generic historical receipt does not
by itself pin content. A revision named by current local or backup dependency
evidence remains protected. An expired revision named by that evidence is not a
candidate. External readers, untracked pins, undisclosed backups, and local
journals outside the declared paths remain unknown; the operation does not
convert their absence into evidence.

## Physical root set

After the cut, the authoritative retention root set is:

| Subject | Retained physical references |
| --- | --- |
| Current head and its snapshot | Every authenticated metadata, index, and content reference |
| Every non-reclaimed historical revision | Its complete retained snapshot graph |
| Reclaimed revision | Only its revision identity and tombstone history record; no snapshot or content references |
| Receipt records | Receipt metadata, but no content references |
| Ordered change records | Change metadata, but no content references embedded in `before` or `after` nodes |
| Declared local and backup dependencies | The exact retained revisions they name |
| Preparation output not published by any retained root | None |

Only the current head's retained-history index defines which historical snapshot
descriptors are live. History indexes nested inside older snapshot descriptors
are themselves retained metadata, but their values do not recursively create
additional snapshot roots. This prevents an old copy of the history index from
resurrecting a revision whose current history entry has been reclaimed.

Receipt and change metadata stay reachable from every retained snapshot that
loads those roots. Their embedded `ContentDescriptor` values are historical
metadata: they identify logical content but do not independently retain content
indexes or payload extents. A raw `ContentDescriptor` obtained from a change
record, an expired revision, or a prior prepared-content response is therefore
not a retention token. A cold authenticated read through such a descriptor can
fail after reclamation. Only a readable revision, protected revision, or
protected frozen request keeps its content readable.

This is a lifetime change from the current reachability traversal, which follows
all expired history and all content descriptors in change records. The
implementation must extend the existing authenticated traversal with an explicit
retention policy; it must not add a second metadata scanner or infer roots from
pack keys.

`visible_uncovered_bytes`, `root_external`, and a partial or complete inventory
are accounting observations. None is a deletion proof. Eligibility requires
zero references from the complete retention root set.

## Atomic retention cut

The cut is a bounded, explicit authority mutation. Its input binds the
filesystem, one expected-latest revision, and a sorted unique set of expired
candidate revisions, using the existing 256-revision maintenance bound. It
validates the declared local and backup evidence before publication.

For every candidate, the cut replaces the historical snapshot reference with an
authenticated reclaimed tombstone while preserving:

- the revision token in the retained history;
- the `Expired` fresh-lookup result;
- receipt identity, request digest, revision, and ordinal;
- ordered change metadata; and
- every readable or protected snapshot.

The cut adds one new logical revision and a receipt through the existing
authority mutation pattern. It does not change the namespace, generations,
logical content identities, or readable history.

The publication point is the authority's existing atomic cell:

- object authority: one conditional `.yinyang/head` replacement;
- metadata service: one SQLite `BEGIN IMMEDIATE` transaction and `COMMIT`.

Before that point, every expired descriptor remains a physical root. After that
point, a candidate's tombstone is the sole logical record of the revision. A
crash or unknown cut response never authorizes the sweep; the authority is
reobserved until the receipt or all tombstones establish whether the cut
committed.

A tombstone is not a new public revision status. Fresh observation still returns
`Expired`; a revision that never existed still returns `NotFound`. Reports may
distinguish physical retention internally so an intentional tombstone is not
reported as corruption or partial reachability.

The object profile must encode the history value as a retained snapshot
reference or a reclaimed tombstone. The metadata-service schema must retain the
same distinction per revision. Both require new experimental profiles and
rejection of older profiles; exact identifiers are an implementation decision.

Without this cut, the concrete pack format still contains an expired snapshot
reference and the existing traversal must treat it as a root. Deleting that
pack would leave an intentional absence indistinguishable from corruption and
would make an exact replay fail without a typed boundary. Keeping every expired
root instead would not satisfy the first useful scope.

## Whole-pack sweep

The sweep starts only after the cut receipt is committed. It reobserves the
authority and refuses to continue if the latest revision is no longer the cut
revision.

1. Traverse the post-cut root set with the existing authenticated metadata,
   index, and content-reference traversal.
2. List current object keys below the filesystem pack prefix and require a
   complete, stable inventory. Missing, truncated, foreign, or unrecognized
   references and any traversal issue abort deletion.
3. Select a pack only when its current-key object has zero references from the
   complete root set.
4. Delete selected packs whole. Reobserve the head before each deletion batch
   and after the final batch. A changed revision stops further deletion.
5. Re-list or stat after an ambiguous delete before claiming that the object is
   absent.

A pack with one live metadata page, receipt, change record, content index, or
payload extent remains. Dead bytes in that pack remain too. This is intentional:
current references name a pack and offset, and no existing publication path
rewrites all references to relocated bytes.

Sweep is not transactional across packs. A crash, timeout, or lost delete
response can leave any subset present. Because no logical state changes during
the sweep, a later run recomputes the same root set and retries any remaining
eligible packs. The operation reports partial deletion rather than creating a
local job, marker, or manifest.

## Shared packs and compaction

Whole-pack deletion is the first contract because it has no reference-rewrite
publication:

- it cannot expose a new extent with an old descriptor;
- it does not need to replace `PackedRef` values throughout namespaces, indexes,
  receipts, or retained descriptors;
- a partially completed sweep is still logically valid; and
- no held reader can observe a half-relocated logical version from a successful
  metadata publication.

Live-data compaction would need to copy live extents into new packs, build all
replacement metadata and content index roots, publish one coherent physical
relayout revision, preserve old roots for held views, and only later reclaim the
old packs. That is a separate physical publication with a larger compatibility
surface. No current caller demonstrates that the bytes stranded in shared packs
justify it. Until such evidence exists, a partially referenced pack stays.

## Failure and retry boundaries

| Point of interruption | Reachable bytes | Required recovery |
| --- | --- | --- |
| Before cut commits | All pre-cut history and content roots remain authoritative | Correct evidence or abandon; no deletion |
| Cut outcome Unknown | The old or new head may be authoritative; tombstones are all-or-none | Resolve the receipt or reobserve the candidate history; never sweep an unresolved cut |
| Cut committed, before sweep | Tombstoned snapshot and content roots are no longer authoritative but bytes remain present | Recompute the post-cut root set and sweep |
| During sweep | Every retained root is untouched; already deleted packs are irreversibly absent | Rerun; do not expect a sweep rollback |
| Delete response unknown | Absence may mean this attempt or an earlier attempt succeeded | Observe current-key absence, then retry safely |
| Authority revision changes | The stopped-process or writer-fence precondition was violated | Stop further deletion and report the observed revision transition |

The cut is one atomic logical publication. The sweep is idempotent physical
cleanup. No recovery path treats an absent receipt as proof that a cut or delete
did not take effect.

## Receipts, frozen requests, and history

A committed receipt is resolved before its base snapshot is restored. Preserving
the receipt therefore preserves exact replay of an already committed request
without preserving the base revision's physical content. Retaining every
historical receipt does not retain every historical snapshot.

A frozen, prepared, Unknown, Retryable, conflicted, or failed request can still
need its original base and prepared content. The first contract refuses
reclamation when such state exists in the declared local or backup scope. This
is narrower than expiry, but avoids treating content referenced only by local
plan bytes as an orphan.

Frozen requests outside configured local state and supplied backups remain
outside the scope. A caller that needs an expired base must declare or resolve
that work before reclamation. Reclamation does not create a remote pin registry
to discover it later.

## Backend versions and accounting

The sweep deletes only current pack keys. It does not enumerate or delete
noncurrent versions, and it does not call a version-specific deletion API.
Therefore:

- noncurrent versions and delete markers remain unknown;
- visible storage may not decrease when the backend retains versions;
- an old noncurrent head is not a supported publication point and may reference
  deleted packs; restoring it is outside the contract; and
- no report may claim all versions removed or total physical space freed.

The first implementation's acceptance is current-key deletion of authenticated,
whole, zero-root packs. It is not a backend versioning or storage-billing claim.

For a metadata-service authority, this contract removes content packs only. The
SQLite authority database, its historical rows, and its database pages remain
and are not `VACUUM`ed. Reclaiming SQLite metadata requires a separate contract.

## Mechanism necessity

| Mechanism | Confirmed behavior that needs it | Simpler alternative |
| --- | --- | --- |
| Explicit expired candidate set | Logical expiry must remain separate from deletion authorization | A read-only retention plan or automatic policy cannot authorize deletion |
| Per-revision tombstone | A known expired revision must remain `Expired`, not become `NotFound` or corruption, after its descriptor root is removed | Indefinite physical retention preserves the old roots but cannot release expired-exclusive data |
| Atomic cut | A pack can be unreachable according to some readers and still required by an old history root unless the root transition is all-or-none | Deleting first would leave an indistinguishable missing root and break retry semantics |
| Whole-pack-only sweep | Existing references embed pack UUID and offset and there is no reference-rewrite publication | Range deletion is unsupported by the object contract; compaction is a separate larger publication |
| Stopped processes and declared state | Held views, in-memory prepared content, and noncommitted local plans are not remotely observable | A distributed pin/lease protocol or writer epoch does not exist and would add long-term state |
| Refuse noncommitted local work | A prepared-content pack can have no published remote reference but still be required for replay or export | Enumerating and protecting every local prepared descriptor adds a second root-discovery path |
| Preserve receipt and change metadata | Exact receipt lookup and ordered history are existing observable behavior | Dropping them would change logical history rather than reclaim bytes |

# Compatibility and Migration

This RFC adds no runtime behavior. Until a later implementation lands, logical
expiry continues to retain all physical objects, and the current space and
retention reports continue to report unknown noncurrent, external-reader, and
reclaimable state.

The current experimental object and service profiles have no physical tombstone.
An implementation must bump their profiles and reject older formats.
Pre-release policy does not require migration, dual readers, or mixed old/new
writers. A volume must be reclaimed only by binaries that support the new
profile.

After successful reclamation, tombstoned content is not recoverable through the
authority: retrying the delete, observing the revision, or replaying a
noncommitted request cannot reconstruct it. A pre-existing backup or a
noncurrent backend version may still contain a copy, but neither is a guarantee
of this operation.

# Validation

An implementation must demonstrate these contracts on both publication
authorities with deterministic in-memory or local fixtures before using a real
backend:

- expired-only content packs become eligible after one cut, while the revision
  remains `Expired`;
- a pack shared by a readable snapshot and an expired revision is retained;
- a pack with one live metadata value and otherwise dead content is retained;
- preparation output with zero roots is reclaimed, but this is tested separately
  from the expired-history case;
- a committed receipt remains lookupable after its base content is removed;
- any noncommitted local intent or incomplete install refuses the operation;
- a protected expired revision is not cut;
- cut receipt replay, lost acknowledgement, and all-or-none tombstone
  observation;
- crash before cut, between cut and sweep, and between individual deletes;
- ambiguous delete responses are resolved by observation and retried
  idempotently;
- a changed head aborts the sweep;
- an incomplete traversal, missing reference, truncated pack, or unrecognized
  visible object prevents every deletion; and
- the final sweep leaves every current and readable snapshot byte-readable and
  reports noncurrent backend versions as unknown.

The tests must exercise the same authenticated traversal used by reachability.
They must not construct a pack deletion list by listing byte gaps or by parsing
object keys independently.

# Open Decisions

| Decision | Recommendation | Evidence needed to close |
| --- | --- | --- |
| Tombstone and cut wire/schema shape | Reuse the existing history key and receipt pattern; bump the experimental profiles rather than adding a registry | Encoded vectors for retained and reclaimed values, replay, mixed-status history pages, and old-profile rejection |
| Noncommitted local-state policy | Refuse the whole operation in the first implementation | A demonstrated workload whose pending plans must coexist with reclamation and a bounded way to extract every prepared physical root |
| Content descriptors in change records | Treat them as historical metadata, not content-retention handles; cold reads may fail after the referenced revision is cut | Updated public specs and a test proving a change-only descriptor can become unreadable without changing current/readable snapshots |
| Service authority maintenance | Require the service process to stop and use the existing database admission offline | A service-side fencing design that proves no prepared transaction can publish after the cut |
| Live-data compaction | Do not implement in the first contract | Representative measurements showing substantial reclaimable bytes stranded in partially referenced packs, plus a complete reference-relayout publication design |
| External readers and undisclosed backups | Keep them unknown and constrain the operation to an operator-declared stopped-process scope | A real pin/lease protocol or an equally verifiable external-reader inventory |
