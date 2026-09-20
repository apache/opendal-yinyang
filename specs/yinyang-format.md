# YinYang Object-Storage Profile

Status: experimental implementation of the object-storage binding in
[Transactional filesystem](transactional-filesystem.md). This replaces the
materialized-version API and encoding. Metadata-service publication remains a
target, not an available backend. The byte profile is not an interoperable
release commitment.

## Public boundary

`Fs` (`object::ObjectFs`) owns one filesystem and an OpenDAL operator.
`create` and `open` require an explicit `BackendProfile`: Amazon S3 or MinIO.
The endpoint must provide that deployment's durable immutable writes, strongly
consistent reads and atomic conditional writes. Selecting a profile is an
assertion about the configured endpoint, not a capability discovery mechanism.
Other S3-compatible products are not automatically supported. Capability flags
are additionally checked; the backend must support streaming writes,
create-if-absent and if-match. The MinIO integration test exercises these
operations against a real server. It does not prove every S3-compatible
deployment has the same semantics.

YinYang owns `.yinyang/` below the operator root. External replacement or
deletion of these objects invalidates the contract. Only the head is mutable;
new immutable objects use unique UUID keys and conditional creation. Each
prepared pack is read back and verified before publication can reference it.
Content readiness follows [the content profile](content-profile.md).

`observe_latest` reads the exact head bytes and their conditional token together.
It loads only the snapshot descriptor, not the namespace. `Snapshot` provides
identity lookup, path resolution, directory pagination, prepared published
content, indexed receipt lookup, and ordered changes. `observe_revision` uses
the retained-snapshot index. There is no listing-based discovery, implicit
latest-cache substitution, expiry, garbage collection, or history replay.

`Planner` captures conditions while constructing a deterministic transaction.
`lookup` and `resolve` capture name bindings; `node` captures the complete
logical node; `scan` captures the complete directory membership, including
absence of unseen entries. Creates guard the selected slot and parent kind,
not the parent's membership generation. File replacement guards the observed
logical node state. Removal protects the source relationship and, for a
directory, emptiness. Rename requires an absent destination (except its own
case-folded slot), protects both bindings and relevant ancestor relationships,
and never rewrites descendants. Root removal and movement are invalid.

New IDs derive from the filesystem ID, commit ID, and mutation ordinal under
the BLAKE3 derive-key context `Apache OpenDAL YinYang node identity profile 2`;
the first 16 hash bytes form the ID. IDs are fixed before an attempt. Retained
receipts prevent reuse of a successful creation intent. Identity collision is
rejected. A successful create-and-delete intent still retains its receipt.

Generations are computed once per accepted transaction from its final logical
state. Content relocation does not change node generation. Name spelling is
part of membership state. Counters fail explicitly before overflow. A no-op
may publish a receipt without advancing logical generations.

`commit` and `commit_batch` check receipts before semantic conditions, verify
canonical request digests, and publish namespace, receipt and change roots
together. Batches run in input order; accepted members share a revision and
have distinct cursors. A rejection depending on speculative state is returned
only after successful publication. CAS misses revalidate the original plan,
without reuploading content or executing a callback. Eight failed publication
opportunities return `Retryable`, with a final indexed receipt check.
Transient failures before publication also return `Retryable`.

An ambiguous head replacement remains `Unknown` while its old conditional
token might still succeed. A fresh observed token fences that attempt; retries
then resolve receipts or revalidate conditions. An absent receipt alone does
not turn ambiguity into failure. Callers retain the request and commit identity.
Invalid requests and malformed persistent state return typed errors, not
semantic conflicts.

## Names and ordering

Components are NFC, 1–255 UTF-8 bytes, with no full-path length invariant.
Normalization uses Unicode 17.0.0 (`unicode-normalization = 0.1.25`); full,
non-Turkic case folding uses Unicode 9.0.0 (`unicode-casefold = 0.2.0`), followed
by the same NFC normalization. These exact versions are pinned together for
this experimental profile. Changing either table is a profile change.

Reject `.`, `..`, U+0000–001F, U+007F–009F, `/ \\ : * ? " < > |`,
and trailing ASCII space or dot. After folding, the stem before the first dot
must not be `con`, `prn`, `aux`, `nul`, or `com` / `lpt` followed by
one of `1..9`, `¹`, `²`, `³`. Input spelling is validated, not silently
normalized. Entries sort by unsigned UTF-8 bytes of the folded key.
Directory continuation tokens bind descriptor reference, parent ID, and the
last exclusive key; they cannot be reused with another observation.

## Persistent encoding

All tuples below use Borsh, little-endian integers, one-byte boolean/option
tags, fixed arrays without lengths, and u32 lengths for strings/vectors.
Decoders reject malformed tags, lengths, trailing bytes and invalid names.
`Ref` is the authenticated packed extent defined by the content profile.
Physical references never enter logical content identities or request digests.

- Revision: `(sequence:u64, nonce:[u8;16])`. Each publication allocates a fresh
  nonce and advances the sequence; a revision is not a count of transactions.
  Comparison is lexicographic over sequence and nonce. Index keys use
  big-endian sequence followed by nonce.
- Head: `(YYHEAD02:[u8;8], mode:u8=0, filesystem:[u8;16], root:[u8;16],
  revision:Revision, descriptor:Ref)`, followed by ordinary BLAKE3 of that tuple.
  Maximum head size is 4 KiB. Unknown profiles/modes and legacy `YYHEAD01` are
  rejected; creation never overwrites an existing unsupported head.
- Descriptor: `(YYSNAP02:[u8;8], filesystem:[u8;16], root:[u8;16],
  revision:Revision, roots:[Option<Ref>;5])`, at most 4 KiB. Root order is nodes,
  directory entries, receipts, changes, retained snapshots.
- Node: `(id:[u8;16], generation:u64, executable:bool,
  link:Option<(parent:[u8;16],name:String)>, membership:u64,
  content:Option<Vec<u8>>)`. Directories have positive membership and no
  content; files have zero membership and a YYFILE02 descriptor. Only the root
  has no link. A lookup validates identity, filesystem and parent-link agreement.
- Directory entry: `(name:String,node_id:[u8;16])`. Its key is the parent ID
  followed by folded-name UTF-8 bytes.
- Receipt: `(commit_id:[u8;16],request_digest:[u8;32],revision:Revision,
  ordinal:u32)`, keyed by commit ID.
- Change record: `(receipt:Vec<u8>,changes:Vec<(before:Option<Vec<u8>>,
  after:Option<Vec<u8>>)>)`. Node encodings supply relationship and generation
  details. Keys concatenate revision index bytes and big-endian ordinal.
  Reads are bounded by their pinned snapshot's change root.
- Retained-snapshot value: a Ref keyed by prior revision index bytes. A
  successor records its predecessor descriptor; descriptors never hash themselves.

Every index is an immutable copy-on-write B+ tree with inclusive maximum-key
separators. A page is `(YYINDEX2:[u8;8],level:u8,
records:Vec<(key:Vec<u8>,inline:Vec<u8>,external:Option<Ref>)>)`.
At level 0, records contain inline values of at most 512 bytes or an external
value reference with empty inline bytes. Higher levels have only child refs
with empty inline bytes. Keys are nonempty, strictly increasing, and at most
1,024 bytes. Pages have at most 32 entries and 64 KiB encoded bytes; nonroot
pages have at least 16 entries. A branch root has at least two children; an
empty index has no root. Splits divide at floor(length/2); deletion redistributes
or merges adjacent siblings, then collapses a single-child root. Child levels
decrease by one, and read separators must match the referenced page.
Maximum level is 32; exceeding it is `Unsupported`.

Values over 512 bytes use authenticated immutable extents, with a 16 MiB
implementation resource limit. Oversized requested values are `Unsupported`.
The current writer uses one pack per changed metadata page/value; independent
packing policy can reduce request overhead without changing logical semantics.

## Canonical requests

The request hash is BLAKE3 derive-key with context
`Apache OpenDAL YinYang request profile 2` over Borsh
`(YYREQ002:[u8;8],filesystem:[u8;16],conditions,mutations:Vec<Vec<u8>>)`.
The commit ID is the lookup key, not part of this hash; creation operands bind
the identities allocated from that ID.

Conditions sort by their byte keys and encode as
`Vec<(key:Vec<u8>,expected:Option<Vec<u8>>)>`. A node condition key is
`tag:u8 || NodeId`: tags 0/1/2/3/4 mean kind, logical node state, parent link,
membership generation, and complete logical node respectively. Tag 5 is
followed by a directory-entry key. Absence is `None`.

Expected values are Borsh: kind is a directory boolean; logical state is
`(generation,executable,Option<(content_length,content_hash)>)`; link is
`Option<(parent,name)>`; membership is u64; complete logical node is
`(id,generation,executable,link,membership,Option<(content_length,content_hash)>)`;
entry is its directory-entry encoding. Complete nodes use membership zero
for files. Physical content descriptors are excluded.

Mutations encode in operation order with one-byte tags:
`0,id,parent,name,Option<(content_length,content_hash)>,executable` for create;
`1,id,content_length,content_hash` for content replacement;
`2,id,executable` for attributes; `3,id,parent,name` for rename; `4,id` for remove.
All IDs are fixed 16-byte arrays. The authority recomputes the digest; there is
no API accepting a caller's claimed readiness or digest without verification.

For filesystem ID consisting of sixteen bytes of value 1, no conditions and
no mutations, the request digest is
`811032fb290ade714ee0ca3d8716fabd1f7154e478ae5da1a8b822411513eb4d`.

## Verification and remaining limits

Tests cover deterministic CAS races, logical conflicts, delayed/lost responses,
batch ordering, pinned pagination/history, immutable preparation, range
corruption, B+ tree splits/merges and lookup depth. Fixed-edit regression tests
increase namespace and receipt counts while bounding bytes and requests; they
are cost checks, not throughput benchmarks. A real MinIO test exercises
multipart preparation, range updates, publication, retries and reopen.

There is no metadata-service engine or remote preparation-evidence protocol.
There is no mount runtime, synchronization policy, garbage collection,
compression, encryption or interoperable-profile release. These are not
implicitly enabled by the object-storage implementation.
