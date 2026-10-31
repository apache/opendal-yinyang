- Proposal Name: `stopped_process_expiry`
- Start Date: 2026-11-01
- RFC PR: [apache/opendal-yinyang#131](https://github.com/apache/opendal-yinyang/pull/131)
- Tracking Issue: [apache/opendal-yinyang#78](https://github.com/apache/opendal-yinyang/issues/78)

# Summary

Define a future stopped-process revision-expiry contract for Managed
filesystems. Expiry may make an unprotected revision logically unavailable,
but it must remain distinguishable from a revision that never existed, must
fail closed when protection evidence is incomplete, and must never invalidate a
frozen request, receipt dependency, or supplied backup dependency.

This RFC defines observable semantics only. It does not add an expiry command,
wire or profile change, persistent pin registry, or physical deletion path.

# Motivation

The current retention planner is declarative and non-destructive. It can name
protected revisions and bounded selections, but every retained revision stays
readable and no maintenance operation removes history. Any future expiry
operation needs a contract that an operator can reason about before history
becomes inaccessible:

- which evidence makes a revision protected;
- what happens when any evidence is unknown or unreadable;
- how a reader tells an expired revision from an absent one; and
- why expiry cannot silently break work captured in staging, Sync, replicas, or
  backups.

Without those semantics, deleting or hiding history would turn an incomplete
observation into a safety claim.

# Non-goals

- No wire format, storage profile, or schema change.
- No persistent remote pin registry or lease protocol.
- No expiry mutator, default expiry, background scheduler, or automatic
  maintenance.
- No physical object deletion, pack reclamation, compaction, or GC algorithm.
- No compatibility promise for an unimplemented command.
- No decision about whether a committed receipt or its revision may itself
  expire; that is a separate policy question.

# Proposed Design

## Stopped-process precondition

Expiry is a maintenance operation that requires the operator to stop every
writer, reader, mount, Sync process, and backup activity for the affected
filesystem before invoking it. The operation observes evidence from a stopped
system; it does not infer quiescence from a successful command.

The operation must be explicitly invoked. No retention plan, report, or
bounded selection expires history as a side effect.

## Protection set

The effective protection set is built before any revision becomes a candidate
for expiry. It includes:

- the current revision;
- every revision selected by explicit operator retention (`--keep`, equivalent
  declared retained points for this operation, or a future explicit expiry
  argument);
- remote base revisions recorded by pending, frozen, conflict, Unknown, or
  failed staging handles;
- Managed Sync namespace baselines, committed receipt revisions under the
  current retention default, and the original replay metadata for each
  committed receipt;
- local replica common baselines, per-entry remote predicate revisions, and
  conflict-recorded revisions;
- readable dependencies from explicitly supplied backup packages, including
  content staging bases, namespace baselines, committed receipts, replica
  baselines, and replica conflicts; and
- external readers, undisclosed backups, or untracked pins supplied explicitly
  by the operator.

Unknown external readers, undisclosed backups, and untracked pins remain
unknown. A boolean operator assertion that they do not exist is not evidence
and cannot be converted into safety.

## Fail-closed evidence

Expiry refuses to proceed when any relevant evidence is unavailable, foreign,
corrupt, partial, or contradictory. This includes an unavailable retained
revision index, unreadable staging or replica state, an invalid supplied backup
package, an unverified filesystem binding, or a revision whose dependency
state cannot be observed.

Partial evidence never becomes an implicit empty set. A future expiry command
must report the unresolved subjects and withhold mutation rather than expire a
subset it cannot prove safe.

## Observable read semantics

An expired revision is not the same as a revision that never existed:

- observing an expired revision returns an explicit expired outcome that
  preserves the revision identity;
- observing a revision with no such identity returns the ordinary not-found
  outcome;
- listing retained history may omit the expired revision from readable history
  only when the contract exposes its expired state separately; and
- an operation must not silently reinterpret expiry as corruption, as an
  empty snapshot, or as a missing filesystem.

The exact error enum, API shape, and listing projection are deferred to a
future implementation RFC.

## Dependency invariant

Expiry must never invalidate a readable revision that a known frozen request,
retryable or Unknown publication, local staging handle, Sync namespace intent,
replica predicate, or supplied backup dependency actually needs. If expiring a
candidate would make such a revision unavailable, the operation fails closed
and reports the dependency relationship.

Committed receipt replay metadata, including the original commit identity,
request digest, and outcome, must remain readable and unmodified so an exact
replay remains possible. Preserving that metadata is not equivalent to pinning
the entire snapshot named by every historical receipt. The current
receipt-retention default is unchanged; which receipt revisions, if any, may
later expire is a separate decision.

Receipt and Unknown outcomes remain distinct. Expiry cannot turn an Unknown
publication into a failure or erase the original commit identity.

## Physical deletion is separate

Logical expiry does not authorize physical deletion. Shared packs, noncurrent
backend versions, metadata pages, prepared orphans, external readers, and
undisclosed backups keep physical reclamation a separate contract with its own
evidence and failure semantics. A revision may be logically expired while all
of its physical bytes remain.

# Compatibility

This RFC adds no runtime behavior. The existing retention commands remain
read-only and non-destructive. The current specification continues to state
that outside-policy revisions remain readable and that `--keep` is not a
persistent pin.

# Unresolved Questions

- Which API and error identity should represent an expired revision?
- Should expired revision metadata remain indefinitely observable, or follow a
  separate metadata-retention contract?
- What operator evidence is sufficient to declare undisclosed external readers
  or backups absent, if any?
- How should a future expiry operation report a protection dependency that
  names multiple local or backup sources?

