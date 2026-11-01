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
| SQLite authority | `yinyang-sqlite-2` | Yes | Yes | No | Reject |
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
| SQLite authority | Schema profile, manifest, required tables, and exact column set before WAL or DDL | `Unsupported` without changing the database | `Corrupt` without changing the database |
| Loopback RPC | Frame decode and token authentication, then protocol version | `Unsupported` before command dispatch | `Invalid` |

Authentication failures use the transport's existing `Invalid` response and do
not reveal whether an unauthenticated version would otherwise be supported.
An absent receipt remains distinct from an unknown profile and must not be used
as proof that an uncertain request did not commit.

## Transition limits

The current build has no persistent-state upgrade or downgrade path. A process
may reopen state only when every profile reached by that state is current.
Replaying a retained frozen request and reading its receipt is not a profile
transition. Exporting logical data through an application protocol is separate
from opening old local state.

An older or newer local profile identifier is rejected with `Unsupported`.
A current identifier with a different schema is rejected with `Corrupt`.
No writer performs partial migration, column addition, shape detection, or
profile dispatch.

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

Local staging, Sync, directory-journal, replica, Direct runtime and Direct Sync
state each have one current identifier:

| Local state | Current identifier |
| --- | --- |
| File staging | `yinyang-stage-4` |
| Sync journal | `yinyang-sync-3` |
| Directory publication | `yinyang-directory-2` |
| Directory replica | `yinyang-directory-replica-2` |
| Direct mutation runtime | `yinyang-direct-runtime-2` |
| Direct Sync | `yinyang-direct-sync-2` |
| Backup manifest | `YYBK0001` version 1 |

Every local database contains exactly one profile row with `singleton=1`.
The shared local store validates the current identifier and the exact table and
column sets before journal-mode changes, recovery, or external effects. Local
stores use SQLite WAL with `synchronous=NORMAL`; durability barriers checkpoint
the WAL at the runtime contract points instead of syncing every write.

The authoritative local details are in
[file operation](file-runtime.md), [Sync](sync-runtime.md), [directory
transfer](directory-transfer.md), [Linux Sync](linux-sync.md), and
[directory reconciliation](directory-sync.md). A writer preflights every
existing component before changing any of them. Unknown profiles and partial
known schemas fail before PRAGMA journal-mode configuration, DDL, runtime
cleanup, replica recovery, or restore destination creation. A standard SQLite
read-only WAL open can create an empty `-wal` and can create or update the
derived `-shm`. This is not authority-state mutation: the main database, any
existing WAL/journal bytes, configured journal mode, logical rows, and recovery
state must remain unchanged. `immutable=1` cannot replace this path because it
disables SQLite locking and concurrent change detection.

Focused tests malform current-profile state in place to prove that stale
identifiers return `Unsupported` and current identifiers with missing or extra
columns return `Corrupt`. These tests assert that rejection happens before
journal-mode configuration, schema repair, recovery, or lock creation.

## Conformance evidence

The committed vectors under
[`crates/core/tests/fixtures/format`](../crates/core/tests/fixtures/format)
cover current content proofs and a frozen transaction encoding. Tests consume
the committed bytes rather than regenerating them with the current writer.

These vectors establish current-profile reader and writer agreement. They are
not old-version fixtures and do not establish cross-version compatibility.
