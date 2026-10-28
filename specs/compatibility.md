# Compatibility

**Status: supported matrix for the current experimental profiles.**

This specification fixes which persistent and wire profiles the current
implementation reads and writes. It does not make the surrounding product a
stable release, promise compatibility with an unlisted release, or authorize
mixed-version writers. Release packaging and support for a particular platform
combination remain separate qualifications.

## Terms

A **profile** is the exact set of identifiers, field order, integer encoding,
hash domains, normalization tables, resource bounds, and rejection rules needed
to interpret bytes. A reader may advertise support only when it implements the
whole profile.

**Same-profile compatibility** means two implementations that advertise the same
profile may read each other's complete filesystem or service state and may
reopen it without conversion. It includes retained snapshots, receipts, frozen
requests, and pending facts that the profile defines. It does not imply that a
different profile, authority mode, or local-state schema is readable.

An **unknown profile** is a syntactically identifiable format whose identifier
is not listed below. A **malformed known profile** has the supported identifier
but violates its structure. Unknown profiles return `Unsupported`; malformed
known profiles return `Corrupt` or `Invalid` as appropriate.

## Supported matrix

| Family | Supported identifier | Reader | Writer | Older profiles | Newer profiles |
| --- | --- | --- | --- | --- | --- |
| Authenticated content | `YYFILE02` descriptor, `YYHASH02` index, 64 KiB logical units, content hash context 2 | Yes | Yes | No | Reject |
| Object metadata | `YYHEAD02` mode 0, `YYSNAP02`, `YYINDEX2` | Yes | Yes | Reject `YYHEAD01` | Reject |
| Canonical request | `YYREQ002`, request hash context 2, node identity context 2 | Yes | Yes | No | Reject |
| Frozen transaction | `YYPLAN01` | Yes | Yes | No | Reject |
| Service binding | `YYSERV01` | Yes | Yes | No | Reject |
| SQLite authority | `yinyang-sqlite-1` | Yes | Yes | No | Reject |
| Loopback RPC | Protocol version 1 | Yes | Yes | No | Reject |

The current writer emits only the identifiers in this table. The current reader
does not perform an in-place profile upgrade, downgrade, authority-mode switch,
or mixed-profile write. A retained object, receipt, or frozen request remains
readable only while every profile reached by that state is supported.

All rows are independently versioned. Implementing one row does not imply
support for another. In particular, a valid service binding cannot be opened as
an object head, and content readiness produced by one profile cannot promote a
descriptor from another.

## Admission and failure

Profile admission happens at the first trust boundary and before the operation
can mutate durable state:

| Boundary | Precondition before effects | Unknown result | Malformed result |
| --- | --- | --- | --- |
| Content descriptor | Identifier and fixed field bounds | `Unsupported` before payload reads | `Corrupt` |
| Content hash node | Identifier before tag decoding or payload output | `Unsupported` before bytes reach the destination | `Corrupt` |
| Object head | Exact head bytes and profile/mode | `Unsupported` before head creation or replacement | `Corrupt` |
| Object snapshot and index | Identifier before interpreting fields or child records | `Unsupported` before publication | `Corrupt` |
| Frozen plan | Identifier before authority observation or content import | `Unsupported` | `Corrupt` or `Invalid` |
| Service binding | Exact `YYSERV01` envelope before client acceptance | `Unsupported` | `Corrupt`; a different valid binding is `Invalid` |
| SQLite authority | Schema profile, manifest, required tables, and column order before WAL or DDL | `Unsupported` without changing the database | `Corrupt` without changing the database |
| Loopback RPC | Frame decode and token authentication, then protocol version | `Unsupported` before command dispatch | `Invalid` |

Authentication failures use the transport's existing `Invalid` response and do
not reveal whether an unauthenticated version would otherwise be supported.
An absent receipt remains distinct from an unknown profile and must not be used
as proof that an uncertain request did not commit.

## Upgrade and downgrade limits

There is no generic migration engine. The only guaranteed transitions are:

- reopening a current-profile authority by the same or another implementation
  that advertises every profile reached by the retained state;
- reading a retained current-profile snapshot, receipt, or frozen request while
  its authority remains supported;
- replaying the original frozen request and receiving its original receipt; and
- opening immutable content whose descriptor and all reachable index nodes use
  the supported content profile.

Creating a new filesystem is not migration. Exporting and importing logical data
through an application protocol is separate from opening the old persistent
state. Automatic profile upgrades, downgrade after a newer writer has opened the
state, and live authority-mode changes are unsupported unless another
specification explicitly adds them.

The 64 KiB content verification unit enters logical identity and remains fixed
unless a new content profile is introduced. The merged
[Linux read-only range results](../docs/linux-range-cost-results.md) observed
bounded payloads for 4 KiB reads and a two-unit boundary crossing, together
with 9-11 storage requests and 7.4-9.2 ms p50 for repeated warm reads over one
immutable version. The study did not compare other unit sizes and does not make
64 KiB an optimum. Packing targets, page-cache sizes, batch limits, retry
counts, and other requested-range policy are tunable without changing logical
identity when the authenticated bytes and identities remain unchanged.

## Local state

Local staging, Sync, directory-journal, replica, and backup state have separate
owners and schemas. Their specifications remain authoritative for supported
transitions. The current repository accepts these identifiers:

| Local state | Current identifiers | Existing transition boundary |
| --- | --- | --- |
| File staging | `yinyang-stage-1`, `yinyang-stage-2` | [File-operation runtime](file-runtime.md) defines the supported stage-1 opening path. |
| Sync journal | `yinyang-sync-1`, `yinyang-sync-2` | [Sync runtime](sync-runtime.md) defines the supported sync-1 opening path. |
| Directory publication | `yinyang-directory-1` | [Directory transfer](directory-transfer.md) defines retained intent and receipt recovery. |
| Directory replica | `yinyang-directory-replica-1` | The local-state specification must define its admission and recovery boundary. |
| Backup manifest | `YYBK0001` version 1 | [Linux operations](linux-operations.md) defines the package boundary. |

This table inventories names only. It does not convert a local implementation
gap into a core compatibility promise. A local format change must update its
authoritative specification and this table in the same change.

## Conformance evidence

The committed vectors under
[`crates/core/tests/fixtures/compatibility-v2`](../crates/core/tests/fixtures/compatibility-v2)
were produced by an encoder independent of the Rust implementation from the
profile descriptions at source revision `fd1338c2`. Tests consume the committed
bytes rather than regenerating them with the current writer. They cover content
proofs and ranges, Unicode names and byte order, a frozen request digest,
receipts, object profiles, SQLite manifest data, and malformed or unknown
variants.

These vectors establish same-profile reader and writer agreement. Cross-binary
qualification against the exact old source revision and recovery of retained
history is recorded separately in #79 evidence. Passing a fixture does not
expand the matrix or turn an experimental platform combination into a released
product.
