# Experimental authenticated content profile

Status: implemented by `yinyang_core::data`. This profile supplies the content
and preparation boundary for the transactional filesystem. It is not a claim
that either publication authority is implemented by this module, or that the
profile has been frozen for an interoperable release.

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

The storage backend must provide durable immutable writes and subsequent read
visibility. External mutation/deletion is outside the contract. This in-process
evidence is not an authentication protocol for an untrusted remote caller.
Compression, encryption, retention expiry, and garbage collection are not
implemented by this profile.
