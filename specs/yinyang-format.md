# YinYang Format Core

## Scope

The YinYang Format core defines the filesystem values, their OpenDAL
persistence, and the publication state machine. It owns the materialized
`Tree`, immutable `FsVersion` values, durable commit identities, and the mutable
head. `Fs` operates directly on one `opendal::Operator`; there is no storage
provider abstraction inside YinYang.

The current implementation stores complete materialized versions and retains
all commits.

## Filesystem values

`Tree` is an ordered map from canonical relative `Path` values to `Node` values.
The empty path is the root. Every other path has an existing directory parent.
Each `NodeId` occurs at exactly one path. The root is a directory whose identity
remains stable across filesystem versions.

A node contains its stable identity, node generation, executable flag, and
either a directory body with a membership generation or a `File` body. A file
contains a `ContentId` and ordered `FilePart` values. Parts are non-empty,
non-overlapping, and exactly cover
`[0, File.content.length)`. An empty file has no parts. Every part fits within
the content identified by its `BlobRef`.

`BlobRef` is an opaque persistent reference plus the identity of its referenced
bytes. YinYang persistence constructs and verifies it. Filesystem values do not
interpret object keys or integrity metadata inside the reference.

Paths use normalized portable components. They are relative, NFC-normalized,
at most 4096 bytes, and contain components of at most 255 bytes. Empty,
dot-relative, control-character, Windows-reserved, and trailing-space or
trailing-dot components are rejected. A directory cannot contain two names with
the same Unicode case-folded NFC form.

## Successor tree contract

New nodes start at node generation 1; new directories also start at membership
generation 1. A stable `NodeId` cannot change between directory and file.

Rename preserves the node identity and node generation. A change to node
executable state or file state advances the node generation by exactly one.
Unchanged node state preserves its generation.

Directory membership is compared as `name -> NodeId`. A create, remove,
replacement, or rename affecting that mapping advances the directory membership
generation by exactly one. Unchanged membership preserves its generation.

## Versions and commits

`FsVersion.number` is its commit count and therefore its publication order.
Genesis is version 0 and contains no commits. Version N contains exactly one
commit for every version from 1 through N. The ordered position of each unique,
caller-assigned `CommitId` is its published version number.

The head contains the current version's `BlobRef`. Every version reached through
one head history uses the same root `NodeId`.

## OpenDAL contract

The supplied OpenDAL operator is rooted at one YinYang filesystem. It must
support read, write, create-if-absent, and ETag if-match writes. Creation and
open fail with `Unsupported` when any required capability is absent. An ETag
used to replace the head comes from the same read that returned the head bytes;
it is never reconstructed with a later metadata request.

YinYang owns these paths below the operator root:

```text
.yinyang/head
.yinyang/versions/<digest>
```

`<digest>` is the 64-character lowercase hexadecimal BLAKE3 digest of the
complete encoded version object. A version `BlobRef` contains that object path
and a `ContentId` covering the complete encoded object. Version writes use
create-if-absent. An existing object at the same content-addressed path is
accepted after its length, digest, and encoding are verified. Every later read
performs the same verification.

The head is created with create-if-absent and replaced with ETag if-match. A
condition mismatch is a publication conflict. Missing, truncated, oversized,
or unverifiable referenced objects are corrupt data. Direct external writes
under `.yinyang/` are outside the format contract.

## Persistent encoding

The payload uses [Borsh encoding](https://borsh.io/). Integers use little-endian
fixed-width encoding. Collection, string, and byte-sequence lengths are `u32`;
enum discriminants are `u8`; booleans use one byte, with 0 for false and 1 for
true.
Strings contain UTF-8 bytes. Fixed byte arrays contain their bytes without a
length. Fields appear in the order shown below, and readers reject unknown
discriminants, invalid booleans, truncated values, and trailing bytes.

```text
VersionBody {
  entries: [Entry],
  commits: [[u8; 16]],
}

Entry { path: string, node: Node }

Node {
  id: [u8; 16],
  generation: u64,
  executable: bool,
  body: Dir { entries_generation: u64 } | File(File),
}

File { content: ContentId, parts: [FilePart] }

FilePart {
  start: u64,
  end: u64,
  blob_offset: u64,
  blob: BlobRef,
}

BlobRef { reference: [u8], content: ContentId }
ContentId { digest: [u8; 32], length: u64 }
```

Entries are encoded in ascending canonical `Path` order. File parts and commits
retain their logical order. `Dir` has discriminant 0 and `File` has
discriminant 1. A version object is `YYVER001 || borsh(VersionBody)` and is
limited to 64 MiB including the magic. The head is
`YYHEAD01 || borsh(BlobRef) || BLAKE3(magic || payload)` and is limited to 4
KiB. The head checksum covers its magic and payload, excluding the checksum
itself.

## Lifecycle

Creation writes an immutable genesis version and creates the head if it does not
exist. Concurrent creators reopen the winner.

Observation reads the head and its condition, reads the referenced immutable
version, and verifies that its root identity belongs to the opened filesystem.

Commit accepts an observation, caller-assigned `CommitId`, and complete successor
tree. It validates the successor, constructs version `current + 1`, persists the
immutable version, then conditionally replaces the head. A successful replacement
returns `Committed`.

When replacement loses a race, the core observes the new current version. The
same `CommitId` in its commits means the earlier attempt succeeded and also
returns `Committed`; otherwise the result is `Conflict`. When replacement
returns an error after the storage system may have accepted it, the core
performs the same commit lookup before returning the storage error. This makes
retry safe when the publication response is lost.
