# Authenticated content profile

Status: `YYFILE02` / `YYHASH02` is the supported content profile for the current
experimental implementation. It supplies the content and preparation boundary
for both publication authorities. The profile identifier, hash context,
verification unit, field order, and proof encoding are fixed. Release support
for a frontend or platform remains separate from this format compatibility.

## Canonical identity

The maximum verification unit is 65,536 logical bytes. Split input at fixed
multiples of this size, independent of upload chunk boundaries. The last unit
may be partial. Every hash below uses a fresh BLAKE3 derive-key hasher with the
context `Apache OpenDAL YinYang content profile 2`. Integers in hash inputs are
unsigned 64-bit little-endian values. Tags are the exact ASCII bytes shown,
without a terminator or length prefix.

- Empty subtree: hash `empty`.
- Leaf at zero-based block position P: hash `leaf || P || length || bytes`.
- Internal subtree beginning at P with K leaves: split at the largest power of
  two strictly smaller than K; hash `branch || P || K || left_hash || right_hash`.
- File identity: hash `file || logical_length || subtree_hash`. Empty files use
  the empty-subtree hash and have no extents or index root.

`ContentId` is the profile identifier, logical length, and file hash. The profile
identifier is implicit in the `YYFILE02` descriptor. Packing and physical index
bytes never enter these logical hashes. Fixed-offset replacement keeps length
and block boundaries unchanged and only rebuilds affected leaves and hash paths.

`ContentId::calculate` applies the same canonical profile to a local byte stream
without writing storage. It uses one verification-unit buffer and a logarithmic
hash frontier. Directory transfer uses this to reuse unchanged published content.

The profile is `YYFILE02` with content hash context
`Apache OpenDAL YinYang content profile 2`. Readers and writers that advertise
it use the same logical identities. Changing the unit size, context string,
domain tags, integer widths, tree split, or descriptor/index fields requires a
different profile identifier; existing identities are never reinterpreted
through new tuning.

The 65,536-byte verification unit enters those logical identities. The merged
read-only FUSE range study
[retains this identity](../docs/linux-range-cost-results.md): 4 KiB reads on
8 MiB and 32 MiB files transferred at most 0.077 MiB of response payload and
crossing one unit boundary transferred 0.133-0.140 MiB, rather than
materializing the file. Repeated warm reads through one immutable version still
issued 9-11 storage requests and took 7.4-9.2 ms p50, while the former
full-staging path issued no requests and took 0.07-0.50 ms. That study observed
the existing 64 KiB unit; it did not sweep alternative unit sizes or establish
an optimum. Cache bounds, packing, and batching remain policy and do not change
the profile when persisted bytes and logical identities are unchanged.

Conformance vectors (lowercase hexadecimal):

| Input | Hash |
| --- | --- |
| Empty file identity | `8b6e8e7afc13ad19dbda880a74217bc132f02679c34b4338cd318b6c264c7714` |
| Leaf 0, bytes `abc` | `47134cf0c0612369f04c52aff76036d23a91db22a5cc8f15a4589cec1f7f9cb5` |
| Leaf 1, bytes `x` | `4569163fa8678830f8c5b4814cb162d80d2e6044c323b593fd6b8231905b6e9f` |
| Branch at 0 with two leaves: 65,536 bytes of value 7, then `x` | `4a9449de9b3ed735f25145e7ec1dbf209b8b698092e29a7f476215dcd1c8ced3` |

## Physical references and proof encoding

All structures use Borsh field and tuple encoding, without derive macros.
`PackedRef` is `(object_uuid: [u8;16], offset: u64, length: u32,
blake3_digest: [u8;32])`. The digest is ordinary BLAKE3 of exactly the referenced
encoded bytes. An object key is derived only as
`.yinyang/v2/<filesystem-uuid>/packs/<object-uuid>` using lowercase compact UUIDs.
There are no caller-controlled path strings. Offset plus length must not overflow.

`Child` is `(logical_subtree_hash: [u8;32], reference: PackedRef)`.
A descriptor is `([u8;8] = YYFILE02, filesystem_uuid: [u8;16], logical_length:
u64, root: Option<Child>)`. Empty content has no root. Hash-index nodes are:

- Leaf: `([u8;8] = YYHASH02, tag: u8 = 0, data: PackedRef)`.
- Branch: `([u8;8] = YYHASH02, tag: u8 = 1, left: Child, right: Child)`.

Descriptor and index-node encodings must fit 512 bytes. Data extents contain raw,
uncompressed bytes and fit one verification unit. The reference authenticates
the index node independently, and child hashes authenticate selected data all
the way to the descriptor's logical identity. Expected tree shape is derived
from file length and block positions, not supplied by an untrusted node.
Trailing bytes, unknown tags, invalid lengths, and invalid proofs are rejected.

Data and hash-index nodes may share a container. The writer appends data and
nodes while keeping only a logarithmic frontier and bounded transfer buffers.
Partial reads fetch only relevant index nodes and complete intersecting units.
Each unit's physical and logical hashes are checked before releasing its
requested bytes. A failure in a later unit may leave a verified prefix. Unread
units are not scrubbed as a side effect of a range read.

Unknown descriptor or hash-node identifiers return `Unsupported` before payload
bytes are read or released. A malformed record carrying `YYFILE02` or
`YYHASH02` returns `Corrupt`. There is no `YYFILE01` reader, automatic upgrade,
or downgrade. The current writer emits only `YYFILE02` and `YYHASH02`.

`Snapshot::open_file(NodeId)` binds a `FileVersion` to published node metadata
and its revision without fetching payload. Its range reads reuse this profile's
verification and do not prepare content or materialize local staging. Content
and metadata stay pinned across concurrent namespace and file changes; a fresh
snapshot is required to observe a newer version. This is an immutable reader,
not a writable file handle or a persistent offline cache.

`FileVersion` range reads may reuse verified proof nodes and data units from a
process-local cache owned by their existing `DataStore` binding. A hit returns
the previously authenticated original bytes for the exact immutable descriptor;
it never returns bytes from a different version and never turns cached presence
into preparation authority. A later cache scope or cold miss still detects
remote corruption before delivering that new range.

Each cache key contains the exact content descriptor and `PackedRef`, so
different logical versions or descriptors cannot share an entry. Bytes enter the
cache only after the physical extent digest and logical proof checks succeed. A
hit is decoded and authenticated again against the pinned tree; an invalid entry
is removed and returned as corruption. Errors and unverified bytes are never
cached.

The payload bound is 16 MiB across at most 4096 entries per `DataStore`, with
least-recently-used eviction. Map and allocation overhead are additional.
Clones and concurrent `FileVersion` readers of the same binding share this
budget. The cache ends with the process and is never persisted. Eviction,
restart, or a fresh logical version requires storage reads again.
`DataStore::import`, content preparation, and fixed-offset updates intentionally
remain uncached: cached read-serving evidence cannot promote untrusted
descriptors to preparation authority or replace their independent storage
verification. The backend immutable-write contract still applies; externally
deleted or corrupted ranges may remain readable only when their previously
verified bytes are still cached.

## Preparation authority

`ContentDescriptor` is serializable and untrusted. `PreparedContent` is not
constructible or deserializable by callers: it binds the descriptor to a
process-local `DataStore` authority shared by clones. A separately created data
binding cannot produce handles accepted by that authority, even if given the
same filesystem UUID. `import` verifies all units of an untrusted descriptor
before issuing evidence. Metadata authorities must only recover published
evidence from authenticated records in their own publication lineage.

The conservative OpenDAL binding streams and hashes each newly written pack,
closes it, and verifies the acknowledged stored bytes once during preparation.
An incomplete, ambiguous, missing, or corrupt upload returns no handle. Once
issued, `accept` checks authority without any storage IO. Fixed-offset updates
use a prepared handle and read only affected old units; shared subtrees retain
their previous readiness evidence. A serialized descriptor must first be
imported, not promoted through a no-op update.

`FileVersion::prepared_content` recovers readiness for an exact selected
published file version without reading payload. `ContentEdit` describes one
fixed-offset replacement whose range length equals its bytes.
`Authority::prepare_overwrite` accepts that exact `FileVersion` plus a set of
edits, and rejects an edit outside the logical length, overlapping edits, or a
base from another data binding. The object and embedded-service bindings apply
all edits in one new immutable pack while reusing unaffected subtrees.
`DataStore::overwrite_prepared` remains the single-edit form of the same trusted
operation.

`Authority::prepare_update` extends the trusted base operation to a final
length. Its inputs are the exact published base, `preserved_prefix`,
`final_length`, and bounded non-overlapping `ContentEdit` values.
`preserved_prefix` must not exceed either the base or final logical length. The
output starts with `base[0..preserved_prefix]`, zero-fills
`[preserved_prefix..final_length)`, and then overlays the edits. An edit must
lie inside the final length and has the same byte length as its range. Base
bytes at or beyond the preserved prefix never reappear, including when a
truncated file is extended later. Edits may explicitly provide bytes in the
zero-filled suffix. The resulting canonical identity is identical to preparing
the complete logical stream with the same bytes.

The implementation may reuse an old subtree only when its start position, leaf
count, and final leaf lengths still match the new tree. A partial boundary
leaf, changed final length, or intersecting edit is rebuilt from its
authenticated old unit and new bytes. Unaffected subtrees retain their existing
references and readiness. Zero-filled suffix units are materialized as actual
immutable data, so canonical proofs remain verifiable without a hole
representation.

`MAX_UPDATE_EDITS` is 4096 non-empty edits. `MAX_UPDATE_UNITS` is 4096
verification units. `DataStore::update_prepared` rejects a larger update before
calling the storage backend, and `update_is_bounded` exposes the same
conservative rule for callers. The unit count covers new zero units, partial
boundary units, and all units touched by edits; generated branch nodes are
bounded by the same changed-unit budget. A larger zero extension or edit set
uses the explicit complete-source `prepare` fallback rather than converting an
update failure into a different request.

The authenticated loopback service does not turn a returned descriptor into
local evidence by importing the complete file. Its fallible preparation request
names the exact retained revision, NodeId, preserved prefix, final length, and
bounded edits. The service reopens that published version from its own metadata
authority, applies the update through its data binding, verifies the new pack,
records the resulting descriptor as durably ready, and only then returns it. An
unknown or unretained revision, missing or non-file NodeId, and wrong service
binding are rejected before payload reads. A retained older revision remains a
valid base; its original file predicates determine whether later publication
succeeds. The authenticated client may use the returned descriptor as
service-issued readiness, but cannot reinterpret cached read bytes or an
arbitrary serialized descriptor as preparation evidence. Choosing the exact
selected base does not rebase a frozen transaction and does not weaken its
original predicates: publication still returns Conflict when the selected file
state no longer matches.

The storage backend must provide durable immutable writes and subsequent read
visibility. External mutation/deletion is outside the contract. This in-process
evidence is not an authentication protocol for an untrusted remote caller.
Compression, encryption, retention expiry, and garbage collection are not
implemented by this profile.

## Conformance

[`compatibility-v2`](../crates/core/tests/fixtures/compatibility-v2) contains
independently encoded descriptors, proof nodes, ranges, and malformed/unknown
records consumed directly by the core tests. The fixed hash vectors above and
these stored vectors must both continue to pass. The complete reader/writer
matrix and transition limits are defined by [Compatibility](compatibility.md).
