# Transactional Filesystem

**Status: shared target contract; both publication bindings implemented experimentally.**
The [object profile](yinyang-format.md), [metadata service](metadata-service.md),
and [content profile](content-profile.md) describe implemented APIs and encodings.
The persistent profiles are not frozen for interoperable release;
[Open decisions](#open-decisions) identifies the remaining evaluation work.

## Scope and cost model

YinYang provides one transactional filesystem contract and one immutable file
content model through two publication modes: OpenDAL and object storage alone,
or a transactional metadata service with an OpenDAL data plane. A filesystem
selects exactly one authoritative publication mode at creation. Both modes must
preserve the semantics below; their physical metadata representations may differ.
Object-storage publication is the first implementation target.

The target replaces complete path-keyed version objects, embedded cumulative
commit lists, and whole-head optimistic concurrency with persistent indexed
metadata, explicit transaction conditions, and indexed commit receipts. It also
defines range-verifiable content and preparation evidence, so reading a small
range need not verify its entire physical container and committing prepared
content need not download it again.

Costs are evaluated in bytes transferred, durable bytes written, dependent
requests, and contention. Useful work depends on the requested range, changed
metadata, changed content, and predicates needed for correctness. Keeping history
has a storage cost proportional to retained changes. There is no single optimum
for every workload: smaller verification units reduce read amplification but
increase indexing, while larger pages, packs, and publication batches amortize
requests but can increase latency or work per update. The cost bounds below are
design requirements derived from this model, not measured throughput claims.

The records and operations in this document define semantics, not Rust API
signatures or a finalized byte encoding. Mount and synchronization policy sit
above this contract.

## Namespace and snapshots

### Stable identities

The logical namespace consists of two indexed mappings:

```text
NodeId -> Node
(ParentId, NameKey) -> DirectoryEntry { name, node_id }
```

A node contains a stable identity, kind, node generation, executable flag, and
either directory membership state or file content state. Every non-root node has
exactly one parent/name link, also accessible by its identity for ancestor
traversal. This link and its directory entry must agree in every published
snapshot. The root is a directory with a stable identity and no parent.
It cannot be removed or moved.

Every live node is reachable from the root, directory relationships are acyclic,
and a `NodeId` is never reused for a different node. A stable identity cannot
change kind. Hard links are outside this contract. Rename changes the parent/name
relationship and preserves the moved node and descendant identities.
New identities are allocated for a creation intent before its first attempt and
reused on retries, with a uniqueness scheme such as the creating commit identity
and an operation ordinal. Deletion never permits reusing an old identity.

`name` is a portable NFC-normalized component of at most 255 UTF-8 bytes.
`NameKey` is its Unicode case-folded NFC form; keys are unique within a directory.
Empty names, dot-relative names, path separators, control characters,
Windows-reserved names, and names ending in a space or dot are invalid. The
persistent profile must fix the Unicode version and exact normalization and
reserved-name rules so that every writer makes the same decision.

Paths are derived from directory relationships. The empty relative path denotes
the root. The target has no stored full-path length invariant: moving a directory
does not require rewriting or checking every descendant path against the current
format's 4096-byte limit. An interface may reject a request above its documented
resource limit with `Unsupported`; this does not make the stored namespace
corrupt. Operations by identity remain available independently of path rendering.

### Generations

New nodes start at node generation 1; new directories start at membership
generation 1. For each accepted transaction, compare its final logical state with
the state immediately preceding that transaction:

- A change to executable state or logical file bytes increments node generation
  exactly once. Unchanged logical state preserves it.
- A change to a directory's `name -> NodeId` mapping increments that directory's
  membership generation exactly once. Descendant-only changes do not affect it.
- Rename preserves the moved node's generation. Repacking, relocating content,
  rebuilding indexes, or changing physical encoding preserves logical generations.

Changes reverted within one transaction do not advance generations. Distinct
transactions in a batch each apply these rules, even if their combined effect
restores the initial state. Counters must not wrap; exhausted representable
values cause an explicit failure before publication.

### Coherent observations

An observation pins a complete logical snapshot. Lookup, ordered directory scans,
file reads, and pagination against that observation use only that snapshot, even
while writers publish new revisions. Continuation tokens bind the snapshot,
directory, ordering, and last key. A logical `Tree` need not be fully materialized
or fetched to use any of these operations.

`observe_latest` obtains a linearizable observation from the selected authority.
A cached observation is an explicit older snapshot, not an implicit substitute
for a latest read. Nodes, directory entries, content references, and commit
receipts belong to the same filesystem and publication lineage.

A `Revision` identifies a published snapshot in that lineage and has an
authority-defined total order. It is not a commit count and callers must not
assume consecutive numeric values. One revision may contain several accepted
transactions. A commit cursor orders transactions within a revision and across
revisions; intermediate batch states need not be exposed as snapshots.

Published observations, their referenced bytes, and successful commit identities
are retained by default. Neither binding may silently expire them because an
underlying database has a short MVCC retention window. Garbage collection and
retention expiry require a separate explicit contract; they are not part of this
target. Failed preparation or publication may leave unreferenced immutable data.

## File content and physical storage

### Logical content identity

```text
File {
  content_id: ContentId,
  length: logical byte length,
  content_index: immutable authenticated index reference,
}
ContentId = (content_profile, logical_length, logical_merkle_root)
```

A content profile fixes the hash algorithm, domain separation, logical leaf
boundaries, tree shape, length binding, empty-file identity, and proof encoding.
Within a profile, identical file bytes have identical `ContentId` values,
regardless of upload chunking, object keys, packing, compression, or location.
The canonical tree is built over logical bytes, not serialized physical extents.
Its balanced shape is derived from logical length and leaf positions so unchanged
subtrees can be reused and proof depth grows logarithmically with leaf count.
The file's declared length must equal the length bound into its identity.
Content-profile changes are format changes, not ordinary repacking.

The content index maps logical ranges to immutable physical extents and supplies
the hash subtrees or proofs needed to authenticate them against `ContentId`.
Index pages are independently authenticated from the pinned file reference.
Extents exactly cover `[0, length)`, without overlaps or gaps; zero-length files
have no data extents. Readers check lengths, offsets, arithmetic bounds, and
proof structure. A self-consistent extent checksum without a link to the pinned
logical root is insufficient.

Persisting reusable leaf hashes and internal subtrees allows a fixed-offset
overwrite to update affected verification units and index/hash paths. A small
overwrite must not require hashing every unchanged byte just to obtain its new
`ContentId`. This bound does not extend to insertion that shifts every subsequent
logical block boundary.

### Containers and verified ranges

A physical object is an immutable container. It may pack data from several files
and independently addressed content or metadata index pages. Each referenced
extent identifies its container, offset, encoded length, decoded length, and
integrity information. Compression and encryption, when supported by the chosen
profile, must allow each verification unit to be decoded and authenticated
independently. A whole-object checksum may supplement range verification but must
not be the only way to read one extent.

Let `b` be the profile's maximum logical verification unit. A read of `r` adjacent
logical bytes must be able to fetch and verify `O(r + b)` data bytes, plus its
index/proof metadata, independently of the size of the packed objects. Physical
encoding must impose a bounded overhead per unit for the equivalent transfer
bound. The index may point directly into a pack; a reader must not scan the pack
or download an index of unrelated contents to locate that range.

Every returned byte must be authenticated against the pinned file version before
release. Streaming may release verified units incrementally; a later I/O or
integrity failure may leave the caller with a verified prefix and an error.
Reading a range does not establish the health of unread content elsewhere in the
container. A consumer needing atomic local file replacement must stage a complete
read before exposing the replacement. A separate scrub can verify all content.

Moving or packing bytes changes their physical references, not the logical
content identity. Replacement references become visible through the same
publication authority and must satisfy preparation requirements. Old references
remain usable by retained snapshots. Logical transactions must tolerate such
physical maintenance when their logical conditions still hold.

### Prepared content

Publication may reference newly written content only after the responsible
storage binding establishes that its bytes and indexes are complete, immutable,
durable under that backend's documented guarantee, and readable by subsequent
readers. This evidence is a prepared-content handle. It binds the filesystem,
content identity, referenced extents, and the authority that established it.
Metadata pages referenced by publication obey the same readiness requirement.

For a controlled upload, the binding computes hashes and lengths while streaming,
completes the immutable writes, and obtains acknowledgements with sufficient
integrity validation to establish that the stored bytes match the prepared
identity. A writer-close acknowledgement without the needed integrity guarantee
is insufficient. Where the backend cannot provide equivalent evidence, the
binding validates the stored bytes during preparation or import. Once evidence
exists, commit must not require another full read solely to establish readiness.

References from a validated published snapshot may reuse its readiness evidence.
An arbitrary caller-constructed reference, a caller-provided checksum, or an
untrusted claim that upload succeeded is not such evidence. A metadata service
must authenticate or independently validate evidence crossing its process or
trust boundary; an opaque handle safe inside one process is not sufficient for
an untrusted remote caller.

Data, hashes, and metadata may be prepared concurrently or packed together when
dependencies allow. Only publication waits for the complete transitive set of
referenced bytes to be ready. An incomplete or ambiguous upload produces no
prepared handle; it may leave an orphan but cannot be published. Preparation
does not itself make data visible in the filesystem or consume a commit identity.
Readiness of previously published subtrees follows from their authenticated
references; a commit need not traverse all shared content to reestablish it.

## Transactions

### Request and validation

```text
Transaction {
  commit_id,
  request_digest,
  read_conditions,
  mutations,
}
```

The caller chooses a unique `CommitId` within the filesystem before its first
attempt. `request_digest` binds a canonical representation of the original
logical request: filesystem identity, all conditions and mutation operands,
logical content identities, and intended attributes. It excludes transport
retries, replacement pack locations, and physical metadata roots. A binding must
verify the digest against the request rather than trust a caller-supplied digest.

A transaction is a deterministic set of logical mutations with explicit
conditions, evaluated against one current snapshot. Read conditions capture
every fact on which the requested result depends: record values or generations,
path bindings used to choose a target, absence of a name, a scanned key range,
directory emptiness, and relevant ancestry. Protecting only the write set does
not prevent write skew or phantom entries. The high-level operation planner owns
capturing these dependencies; an authority must also enforce invariants even
when accepting lower-level requests.

Validation and mutations take effect atomically in a serial order consistent
with completed commits. The authority validates the original conditions against
the state preceding the transaction, then checks namespace and generation
invariants against its final candidate state. A caller
that read a broad directory listing and depends on that listing must include
that predicate; independent creates need only protect their relevant name slots
and parent identity/kind. A parent's membership generation must not become an
implicit condition for every create.
Global invariants may be established from a valid base snapshot and checked
changes; bounded edits must not require a full namespace traversal to validate
unchanged records.

Rename validates the source and destination bindings, replacement policy, and
the destination's ancestry at publication. Concurrent moves of directory A under
B and B under A cannot both succeed. This may require ancestor reads and
validation, but not rewriting descendants. Removing an empty directory protects
the emptiness predicate against concurrent insertion. A recursive replacement
that means "replace exactly this observed subtree" protects the entire replaced
scope, including concurrent additions; it cannot silently erase unseen changes.
Such broad operations necessarily validate more state than a single-name edit.

The following outcomes assume no additional caller conditions and retries that
eventually obtain a publication opportunity:

| Concurrent requests | Required behavior |
| --- | --- |
| Update distinct files | Both can commit when each observed file state still matches. |
| Create different names in one unchanged parent | Both can commit; distinct normalized name slots do not conflict. |
| Replace the same observed file with different bytes | At most one commits against that original file state. |
| Create a child while removing its empty parent | At most one commits against those original predicates. |
| Move A below B while moving B below A | Acyclicity is preserved; at most one commits. |
| Repack a file while editing that file | Physical references alone do not create a logical conflict when the edit's logical conditions still hold. |

Retries replay the prepared mutation plan after revalidating its original
conditions. They do not rerun arbitrary user callbacks, repeat external effects,
or reupload unchanged prepared content. A changed semantic condition returns
`Conflict`; choosing new conditions is a new logical request with a new commit
identity.

### Commit receipts and history

Each successful transaction atomically publishes its namespace mutations, commit
receipt, and ordered change record. A receipt binds `CommitId`, `request_digest`,
the resulting published revision, and its commit cursor. Changes identify the
affected logical records and resulting generations, including enough before/after
namespace information to represent a move or removal. Change records preserve
the accepted transaction order even if several transactions share a revision.

The commit index supports lookup by `CommitId` without scanning or copying all
earlier receipts. The change log supports ordered pagination by cursor at a
pinned upper revision. A separately updated mutable receipt index cannot be the
authority for success: a namespace change without its receipt, or a receipt
without the change, would break retry safety.

For the same identity and digest, retry returns the original receipt if already
committed, regardless of whether the original read conditions still hold. The
same identity with a different digest is invalid and must not return that
receipt or apply a second mutation. A known no-op request may still commit a
receipt; it leaves logical generations unchanged.

### Outcomes and uncertainty

| Outcome | Meaning and caller action |
| --- | --- |
| `Committed(receipt)` | Data readiness and atomic durable publication are established; the original result is reusable. |
| `Conflict` | Original logical conditions no longer hold; no publication from this attempt remains unresolved. Replan with a new identity. |
| `Retryable` | The attempt is known not to have published, but contention or temporary unavailability prevented completion; retry the same request and identity. |
| `Unknown(commit_id)` | An attempt may have published or may still publish; query or retry that same identity. |
| `Invalid` / `Unsupported` | The request violates a contract, reuses an identity for different intent, or requires unavailable capabilities. |
| `Corrupt` | Published bytes or metadata fail their required integrity or structural checks. |

An absent receipt after a timed-out write is not proof of failure while that
write could still complete. Cancellation is not rollback. A binding must keep
the outcome `Unknown` until it can establish success or rule out every pending
publication attempt. For object storage, a confirmed move beyond the attempt's
head condition can fence that attempt, after which receipt lookup resolves it.
For a service, resolution belongs to its durable transaction protocol. Retryable
physical contention must not be reported as a semantic conflict. These rules
also apply when a process crashes after publication and before replying.

## Object-storage publication

### Required storage semantics

This mode uses only OpenDAL and its backing storage. It requires:

- Immutable writes at unique keys, range reads, and acknowledged durability and
  visibility sufficient for all objects referenced by a later publication.
- Linearizable head reads and conditional create/replace of that one head.
  Compare-and-swap must exclude both concurrent replacement and ABA, where a
  previously observed condition becomes usable again after intervening updates.
- An opaque conditional token obtained with the exact head bytes observed, never
  assembled by a separate stat request.

OpenDAL capability flags must be checked, but flags alone do not establish these
semantics. A backend failing any requirement is `Unsupported` for this binding.
Mutable external writes to owned keys are outside the contract. Object listing
is not needed to identify current state, resolve a commit, or read history.

### Persistent indexes and head

The tiny mutable head identifies the filesystem, format profile, authority mode,
revision, and an immutable snapshot descriptor. The descriptor roots persistent
copy-on-write B+ trees for nodes, directory entries, commit lookup, ordered
changes, and retained-snapshot lookup. Parent lookup may be part of a node record.
Pages have authenticated references, bounded encoded size, ordered keys, and
bounded fanout; values too large for a page use independently referenced immutable
values.

The retained-snapshot index maps prior revisions to authenticated descriptor
references; the head supplies the current descriptor. Before replacing a head,
the writer adds that head's descriptor to this index. Thus a snapshot need not
contain its own hash, and historical lookup requires no linear history walk.
A candidate revision token is allocated before its descriptor is encoded and
does not derive from that descriptor's hash. An observation pins the descriptor
reference obtained from the authoritative head or a published snapshot index.

Writers share unmodified pages between snapshots and write only changed paths,
required splits or merges, and new records. Tree balance and occupancy rules
must bound lookup depth; readers must not replay an ever-growing delta chain to
obtain the current state. Pages may be packed, provided writing a changed page
does not require rewriting an unchanged pack. Enumeration reads only the pages
covering the requested range. A head update always installs a fresh revision and
cannot reinstall an old head value; restoring old logical state is a new commit.

The snapshot descriptor and head integrity envelope bind the roots to their
filesystem and revision. Readers reject unknown profiles, malformed indexes,
invalid checksums, and references outside the permitted storage namespace.
Reference validity includes authenticated length and position within a pack.

### Publication procedure

1. Observe the head and retain its conditional token. Check the commit index for
   the request's identity before evaluating its conditions.
2. Validate the request against that snapshot and construct changed metadata
   paths. Confirm readiness of all new data and metadata references.
3. Write the immutable snapshot descriptor containing both the updated namespace
   and the new receipt/change roots. All dependencies must be ready first.
4. Conditionally replace the observed head. This successful replacement is the
   transaction's publication point. Only then acknowledge `Committed`.

If the condition misses, read the new head and check for the commit identity.
If absent, revalidate the original conditions against the new roots and rebuild
only the metadata affected by this attempt. A missed head condition alone is
not a logical conflict. Bounded automatic retries may return `Retryable` under
contention; they must not silently change the request. An ambiguous replacement
uses the uncertainty rules above and cannot be resolved by a stale cached head.

Creation prepares an empty namespace with its root and indexes, then creates the
head conditionally. Concurrent creators reopen the winner and verify its format
profile and authority mode. A losing creator's immutable objects may be orphaned.

### Batching and physical limit

A publisher may group independent submitted requests into one head replacement.
It checks and applies accepted requests in a deterministic serial order within
the batch, evaluating each against the preceding accepted state. A rejected
request has no effects; accepted requests share the published revision and have
distinct ordered commit cursors. All their data must be ready before publication,
and all are acknowledged only after the head replacement succeeds. Duplicate
identities within a batch obey the same receipt rules as ordinary retries.
If a rejection depends on an earlier speculative batch member, it becomes a
definitive `Conflict` only after that batch publishes; if publication fails, the
publisher must revalidate against a published state before returning that result.

The single head remains a serialized physical publication point. Batching
amortizes one successful replacement across transactions; retry logic eliminates
unnecessary logical conflicts but cannot remove that serialization, wasted
speculative page writes, or latency from batching. There is no guarantee of
starvation freedom under unbounded competing writers. Independent clients gain
cross-client batching only when their requests reach a common publisher; this
mode does not assume a mandatory external coordinator.

## Metadata-service publication

The service implements the same semantic operations: latest observation,
snapshot lookup and scan, commit, commit lookup, and ordered changes. It owns the
authoritative namespace, publication ordering, commit deduplication, and retained
snapshot history. File bytes and content indexes use the shared immutable
content model and may transfer directly through OpenDAL.

The service may store metadata as transactional records and indexes rather than
object-format B+ tree pages. A durable service transaction must atomically:

1. Resolve an already-committed identity or verify the request's digest.
2. Validate all original conditions, including absence and range predicates, and
   protect ancestor relationships needed to enforce acyclicity.
3. Apply mutations, enforce namespace and generation invariants, and record the
   receipt and ordered changes at its resulting revision.

The service checks trusted content-readiness evidence before making this
transaction visible. Its durable transaction commit is the publication point;
after acknowledgement, all new observers can find the receipt and content.
A service response cannot precede durable authority publication. The same
`CommitId` submitted concurrently is applied at most once. Implementations may
batch or partition conflict detection while preserving the shared serializable
contract and coherent snapshots.

A service commit must not depend on replacing an object-store head or
materializing a complete filesystem snapshot in object storage. Checkpoints and
portable exports may be produced asynchronously; they are derived state, not
commit authorities. The service must retain enough durable history to read any
retained published revision after restart; ephemeral MVCC versions alone do not
satisfy this requirement.

A service outage makes new authoritative observations and writes unavailable;
prepared data and explicitly pinned snapshots may still be usable when their
required metadata is available. It never authorizes writes through an older
object-store checkpoint. Service recovery must restore the authority's durable
history and deduplication state before accepting requests. Failure resolution
uses `Unknown` where commit status is uncertain. Service mode moves coordination
into the metadata system; it does not promise unlimited throughput or eliminate
contention on logically conflicting records.

## Compatibility and ownership

This is a replacement format for newly created filesystems. It does not require
compatibility readers, mixed old/new writers, or in-place migration from the
current `YYVER001` / `YYHEAD01` format. New profiles must be distinguishable so an
old filesystem cannot be accidentally interpreted or overwritten as this target.

Both bindings share namespace, content, transaction, retention, and failure
semantics. They need not share metadata page encodings or service database
schemas. Authority identity and mode are fixed for a filesystem; live switching,
dual-authority operation, and automatic fallback between modes are outside this
contract. Moving logical data to a newly created filesystem is separate from
changing the authority of an existing one.

## Performance obligations and limits

Let `N` be the number of live namespace records, `C` the number of retained
commits, `B` index fanout, and `k` the number of logical records changed. The table
states bounds for a resolved, uncontended operation; contention, failed attempts,
proof/index metadata, and retained history remain visible costs.

| Operation | Target work and necessary limit |
| --- | --- |
| Read `r` adjacent file bytes | `O(r + b)` data bytes plus index/proof paths; independent of pack size. |
| Overwrite `d` adjacent bytes at fixed offsets | `O(d + b)` data plus affected hash/index paths; shifting later block boundaries can require more work. |
| Change `k` namespace records in object mode | `O(k log_B N)` metadata pages, plus `O(log_B C)` paths for commit, change, and retained-snapshot indexes and the new change records; shared paths may reduce this. |
| Resolve a path | Work follows its depth and indexed child lookups; stable identities do not make arbitrary paths constant-time. |
| Rename a directory | Update its relationship and affected parents, with ancestor validation; no descendant rewrite. |
| Retry a committed request | Indexed receipt lookup; no data reupload, namespace materialization, or full-content validation. |
| Publish ready data | No mandatory readback of content whose readiness is already established. |
| Retain history | Space grows with changed data, changed index paths, and receipts; history is not copied into each successor. |

The metadata-service binding must permit incremental indexed record reads and
writes without imposing a complete object-store metadata rewrite per commit.
Its exact physical I/O bounds depend on its transaction engine. Predicates over
a whole directory or subtree have a larger validation scope; `k` alone does not
bound the work needed to prove them. Backend minimum request sizes, proof depth,
packing fragmentation, and network round trips affect constants and latency.

An exact initial comparison with an arbitrary local directory still requires
inspecting all relevant names, metadata, and bytes unless a trusted change
journal or previous content identity supplies equivalent evidence. Content
hashing cannot infer equality without examining unknown input. Incremental
uploads can save network traffic independently of this local scan cost.

## Conformance obligations

Implementations of either mode must demonstrate these externally observable
properties; this document does not claim that the current core passes them:

- Concurrent disjoint updates and distinct-name creates survive physical
  publication races; conflicting overwrites, insertion versus empty removal,
  and cyclic moves preserve their stated predicates and invariants.
- A lost success response resolves to the original receipt on retry. Duplicate
  identities never apply twice, and different intent under an existing identity
  is rejected. Delayed publication is not mistaken for a definitive failure.
- Readers remain on their pinned revision across publication, repacking, and
  restart. Batched commits have one coherent visible result and ordered receipts.
- Corruption within a selected verification unit fails before that unit's bytes
  are released. Corruption solely outside the requested units need not affect
  that read. Invalid offsets and proofs cannot escape validation.
- Incomplete uploads cannot publish. Prepared content avoids a second mandatory
  full download, while untrusted imported references cannot bypass validation.
- Holding read size and changed records fixed while increasing pack size,
  namespace size, or history does not introduce whole-pack, whole-namespace, or
  whole-history work beyond the indexed bounds above.
- Service commit acknowledgement does not depend on an object-store checkpoint,
  and checkpoint availability cannot enable a second write authority.

## Open decisions

The content and metadata encoding choices must close before an interoperable
persistent profile is released. Policy tuning may continue independently. These
choices do not defer either publication mode's semantic contract.

| Decision | Recommendation and reason | Evidence needed to close |
| --- | --- | --- |
| Canonical content profile and verification unit | Use a BLAKE3-based canonical logical tree with persisted proof subtrees and fixed logical block boundaries. Choose the unit size by balancing small reads and overwrites against proof/index traffic. | A complete hash and proof definition, empty/partial-leaf and incremental-update vectors, and representative range-read/update cost measurements. |
| Namespace and object metadata encoding | Freeze Unicode normalization/case-folding and reserved names, deterministic record/digest encoding, B+ tree occupancy, page limits, and head/profile identifiers together. Bounded pages and independent references are required by the cost and integrity contracts. | Cross-reader vectors for ordering, request digests, malformed records, index splits/merges, and depth/size bounds. |
| Packing and batch policy | Keep pack targets and batch delay tunable without changing logical identities or semantics. Avoid selecting one universal optimum. | Representative small-file, sparse-update, range-read, and concurrent-writer workloads measuring bytes, requests, latency, and speculative work separately. |

The service storage engine and its wire protocol are binding-specific choices.
An engine is eligible only if it can enforce the transaction, retained-snapshot,
and uncertainty contracts above; it must not require weakening them to fit its
default MVCC or retry behavior.
