- Proposal Name: `history_expiry`
- Start Date: 2026-11-01
- Tracking Issue: [apache/opendal-yinyang#78](https://github.com/apache/opendal-yinyang/issues/78)

# Summary

Implement explicit, non-destructive logical expiry for Managed retained
revisions. Expiry is opt-in, defaults to keep-all, and remains distinguishable
from a revision that never existed. It preserves receipt replay metadata,
physical bytes, original retained-snapshot references, and already-held
immutable observations.

This RFC resolves the implementation questions left open by
[RFC 0042](0042_stopped_process_expiry.md). RFC 0042 remains immutable; this
document supplies the current API, persistent-state, wire, and compatibility
contract.

# Motivation

The current retention plan can identify protected revisions, but every
retained revision remains readable. A useful maintenance path needs to make an
explicitly selected revision logically unavailable without turning an
incomplete local observation into a data-loss claim.

The operation must work across the object authority, the embedded metadata
service, and the authenticated service client. It must also preserve the
existing distinction between:

- an expired revision, which still has authenticated history identity;
- a revision that never existed, which remains `NotFound`;
- a committed receipt, whose original identity, digest, and outcome remain
  resolvable; and
- a physical object, which expiry never deletes.

# Non-goals

- No default expiry, background scheduler, or implicit retention policy.
- No physical deletion, pack reclamation, compaction, or noncurrent-version
  cleanup.
- No persistent remote pin or lease service.
- No global proof that undisclosed readers, backups, or local roots do not
  exist.
- No unexpiry operation in this iteration.
- No change to ordinary successful fsync or publication hot paths.

# Maintenance Scope

The operator explicitly selects one authority and a bounded candidate set while
all writers, readers, mounts, Sync processes, and backup activity for that
fixture are stopped. The local caller holds its existing local lease from the
evidence scan through the Core mutation. Core does not inspect local paths and
does not own that lease.

The checkable scope consists of:

- the authority identity and one exact `expected_latest` revision;
- the configured local staging root;
- every explicitly supplied backup package;
- every explicit requested pin; and
- every authority revision actually needed by readable evidence in that scope.

Unknown external readers, undisclosed backups, and untracked pins remain
unknown. They are not represented as `false`, empty, or absent. Logical expiry
is therefore not a claim of global isolation or deletion eligibility. Because
this operation changes visibility only, an unknown reader receives an explicit
expired result instead of silently observing a missing volume.

The local lease is necessary because an authority CAS cannot prevent local
staging from changing after inspection. Core separately fences authoritative
changes with `expected_latest`.

# Protection Set

Expiry is refused unless every candidate is outside the authority-side
protection intersection.

Core treats the current latest revision as permanently protected. The caller
must provide protected revision identities for every readable dependency in
its validated scope, including:

- all ready durable file handles, including a clean handle with no pending,
  frozen, conflict, or error state. A clean handle is recoverable by durable ID
  and may later read an untouched range or publish another change from that
  base;
- staging bases and frozen requests;
- Managed Sync namespace baselines, retryable or Unknown work, and live
  committed intent evidence;
- replica baselines and conflict-recorded predicates; and
- readable bases and local dependencies recorded in supplied backup packages.

The caller owns the source/reason mapping and human diagnostics. Core receives
only revision identities. A protected revision absent from retained history or
already expired causes a structured refusal rather than a partial mutation.

A committed receipt record by itself does not pin its historical snapshot.
Callers that require continued snapshot reads add an explicit protected
revision. Exact receipt resolution remains available without that pin.

The canonical evidence digest is opaque to Core. It binds the caller's audited
local evidence to the operation record; it does not claim that the evidence is
complete or safe.

# Request And Validation

The public Core request is:

```text
ExpiryRequest {
    volume: NodeId,
    id: ExpiryId,
    expected_latest: Revision,
    candidates: [Revision],
    protected: [Revision],
    evidence_digest: [u8; 32],
}
```

Each candidate and protected set is sorted, deduplicated, nonempty where
required, and bounded to 4096 entries. Core computes the canonical request
digest from the volume, operation identity, expected latest, both revision
sets, and evidence digest.

`Authority::expire_revisions` validates in this order:

1. Decode and validate the canonical request and authority binding.
2. Resolve an existing operation record by `ExpiryId`.
3. If the same operation and digest are already committed, return the original
   receipt before considering the now-stale `expected_latest`.
4. Reject the same operation identity with a different digest without writing.
5. Compare the current latest revision with `expected_latest`; a mismatch
   returns `Conflict` and is not rebased.
6. Check candidates for current, missing, or already-expired identities.
7. Check protected identities for missing or expired state and reject any
   candidate/protected intersection.
8. Publish all markers and the new maintenance revision atomically.

The result is:

```text
ExpiryOutcome =
    Committed(ExpiryReceipt)
  | Refused(ExpiryRefusal)
  | Conflict
  | Retryable
  | Unknown(ExpiryId)
```

`ExpiryRefusal` contains the current latest revision and authority-side
conflicts `Current`, `Missing`, `AlreadyExpired`, `Protected`,
`ProtectedMissing`, or `ProtectedExpired`. The caller maps protected revision
identities back to its complete source/reason mapping.

# Read And History Semantics

An expired revision has a new domain result:

```text
ErrorKind::Expired
Error::expired_revision() -> Option<Revision>
```

A fresh `observe_revision(expired)` returns `Expired` with the exact revision
identity. `observe_revision(unknown)` continues to return `NotFound`.

The existing retained-identity page continues to enumerate all history
identities, including expired ones. The detailed history API adds:

```text
HistoryEntry { revision, state }
RevisionState::Readable | RevisionState::Expired(ExpiryMarker)
```

The current latest revision cannot be expired. The expiry operation records
the prior latest snapshot and publishes a fresh maintenance revision. This
keeps the prior latest reachable and distinguishes a maintenance event from a
namespace mutation without inventing a fake receipt or change record.

Already-held `Snapshot` and `FileVersion` values remain immutable
observations. Logical expiry does not revoke metadata or bytes already
authenticated by those handles, nor does it delete their physical references.
A fresh history observation is required to observe the expired state.

An expiry marker has no TTL. It remains for the lifetime of the authority
lineage. A future metadata-retention or re-admission design may supersede that
lifecycle; this iteration neither removes markers automatically nor revives
them through keep-all or `--keep`.

# Receipt Replay

Expiry never edits receipt or change records. The latest authoritative
metadata retains the original CommitId, canonical request digest, outcome
cursor, and ordered changes.

`Authority::resolve_receipt(plan_bytes)` parses the complete frozen-plan
profile, validates its volume binding, recomputes the canonical request digest,
and only then resolves the receipt by CommitId. It does not trust the digest
field stored in the request header.

If the receipt exists and its digest matches, the original receipt is returned
even if its base snapshot is expired. If the identity exists with another
digest, resolution is `Invalid`. If no receipt exists, resolution returns
`None`; expiry never converts unresolved work into a failure.

This path is for replay and expired-base recovery. Successful ordinary fsync
does not call it and performs no additional history or receipt RPC.

The authenticated service transport applies the same rule without a new
resolver RPC. `Get` and `Scan` for an already-issued lazy `Snapshot` reopen the
retained record reader rather than running the fresh `Observe` expiry guard.
`Command::Commit` attempts normal restore first; only an expired base triggers
complete canonical-plan validation and local receipt resolution. Mixed batches
retain input outcome order, original receipt cursors, and a missing receipt is
never reported as committed.

# Object Persistent State

An authority that has never expired history remains exactly:

```text
YYHEAD02 mode 0
YYSNAP02 roots:
  nodes, entries, receipts, changes, history
```

The first successful expiry performs a one-way transition:

```text
YYHEAD02 mode 1
YYSNAP03 roots:
  nodes, entries, receipts, changes, history, expiry
```

Index pages remain `YYINDEX2`. The expiry index uses tagged keys:

- `0 || revision[24]` selects an expired revision;
- `1 || operation_id[16]` selects the committed operation receipt.

Values use `YYEXP01` and contain the operation identity, canonical request
digest, evidence digest, prior latest revision, and post-expiry revision. The
history index keeps the original snapshot reference, so physical reachability
still traverses the expired snapshot.

Object publication writes all immutable metadata first and changes visibility
with one conditional `.yinyang/head` replacement. A lost acknowledgement is
resolved by reading the operation record. Retry with the same exact request is
idempotent. A pre-publication storage error returns `Retryable`; an ambiguous
head response returns `Unknown` unless the operation can be resolved.

# Service Persistent State

An authority that has never expired history remains on
`yinyang-sqlite-1`. Its schema and normal publication path are unchanged.

The first expiry transaction:

- creates `expiry(key BLOB PRIMARY KEY, record BLOB NOT NULL)`;
- changes the authority profile to `yinyang-sqlite-2`;
- inserts the operation and revision records;
- inserts the maintenance revision; and
- updates latest.

The transaction is one `BEGIN IMMEDIATE` publication. Every validation and
write is inside it. A failed acknowledgement is resolved by reading the
operation record; otherwise the exact request remains `Unknown` for retry.

History queries on profile 1 do not query the expiry table. Profile 2 supports
both the original retained-identity page and the detailed state page.

# Compatibility

The first expiry transition is one-way:

- mode-1 object heads are rejected by pre-expiry readers as an unknown mode;
- profile-2 service databases are rejected by pre-expiry readers as an unknown
  schema profile;
- no downgrade or in-place reversion exists.

Authorities that never expire retain the old formats and remain readable by
older supported readers.

The loopback RPC protocol remains version 1. New commands and responses are
appended without renumbering existing variants. Old clients reject the new
expired response or commands rather than interpreting them as `NotFound`.
`Expired` is appended as the next typed error tag.

# Examples

Successful expiry:

- revisions `r0`, `r1`, `r2` exist;
- a clean durable handle and a requested pin protect `r1`;
- `r0` is explicitly selected;
- `observe_revision(r0)` later returns `Expired(r0)`;
- a never-existing `r9` returns `NotFound`;
- `r1` and the current maintenance revision remain readable;
- the original receipt for a request based on `r0` still resolves exactly;
- no physical pack is deleted.

Required refusals:

- candidate equals the current revision;
- candidate is absent or already expired;
- candidate appears in the protected identities;
- a protected identity is absent or expired;
- the head changes before the CAS or SQLite transaction;
- the same operation identity is reused with another request digest.

# Rejected Alternatives

- **Global pin or lease registry:** cannot prove off-host readers or
  undisclosed backups and adds a new availability and lifecycle service.
- **Sidecar markers without an authority reference:** cannot be published
  atomically with normal history and can partially execute.
- **Encoding expiry in existing history values:** mixes snapshot references
  with visibility state and gives old readers no crisp profile boundary.
- **Pinning every receipt snapshot:** defeats the receipt/snapshot
  distinction and grows history for metadata-only replay.
- **Requiring proof that no unknown external source exists:** converts an
  unknowable fact into a boolean and makes the feature unusable.
