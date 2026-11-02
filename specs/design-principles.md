# Design principles

This specification is the maintained rulebook for how Apache OpenDAL™ YinYang
is built. Every change must satisfy it. A pull request that cannot satisfy a
rule names the rule and the reason in its description; the rule changes here
first, in the same or an earlier pull request, or the change does not merge.

[RFC-0044](../rfcs/0044_architecture_consolidation.md) records why these rules
exist and the decisions behind them.

## Product scope and layering

YinYang maintains every combination below as a long-term product:

| Model | Access | Frontends | Authority |
| --- | --- | --- | --- |
| Managed | Mount, read-only and writable | Linux FUSE, library, native bridge | object or service |
| Managed | Sync | ordinary directory | object or service |
| Direct | Mount, read-only and writable | Linux FUSE, library | storage namespace |
| Direct | Sync | ordinary directory | storage namespace |

One-shot directory transfer, space and retention reporting, and the native
macOS bridge are maintained surfaces built on the same layers.

The code is layered, and a layer depends only on layers below it:

1. **Format core** (`crates/core`): YinYang Format values, content
   verification, transactions, and the `Authority` trait with its object and
   service implementations.
2. **Local state**: the shared SQLite store, durability barriers, and the
   retained-artifact lifecycle used by every runtime.
3. **Runtimes**: the Managed file runtime and Mount, the Direct runtime, and
   one sync engine that serves Managed and Direct Sync through a remote
   adapter.
4. **Frontends**: one FUSE adapter over a backend trait, the CLI, and the
   native bridge. A frontend translates native operations; it never adds or
   weakens a guarantee.

A new combination is an adapter over an existing layer. It does not copy a
runtime, frontend, state store, status schema or error type.

## One implementation per concept

Each concept below has exactly one implementation. Extend it rather than adding
a second one beside it.

| Concept | Single owner |
| --- | --- |
| Volume configuration loading, relative path resolution and admission | `volume` |
| Root and state containment check | the sync engine's local tree |
| Storage operator and service token construction from the environment | `volume` |
| Error kinds, errno mapping and CLI exit codes | `runtime::error` over the core `ErrorKind` |
| SQLite open, schema check, durability mode and row encoding | the local store |
| Retained-artifact storage, release and budget | the local artifact store |
| Durable publication intent states | one intent log |
| FUSE session lifecycle, inode table, mode and flag mapping | `fuse` |
| Sync reconciliation, local scanning and status document | the sync engine |

Two code paths that answer the same question are merged before either one is
extended. Copying a helper into a second module is a defect, not a style
choice.

## Cost scales with the change

- A no-op run, a status call or a metadata callback costs O(changed entries)
  plus a bounded constant. It never costs O(history), O(retained artifacts),
  or O(tree size) per entry.
- Code does not rescan, rehash or re-download a whole tree inside a per-entry
  loop. It keeps per-path fingerprints and rechecks only the path it decides.
- Cheap evidence comes first: compare `stat` identity (device, inode, size,
  modification and change time) before hashing bytes, and hash only when that
  evidence changed or is ambiguous.
- One reconciliation pass publishes one batch when the authority supports
  batches, not one head commit per file.
- Benchmarks and regression tests use realistic file sizes, at least hundreds
  of KiB, and include growth series such as 32 to 256 files or 0 to 40 edits.
  Super-linear growth must fail a test instead of hiding behind 1 KiB
  fixtures.

## Retained local state has an owner, a release event and a bound

- Every piece of local state kept for safety names the event that releases
  it. An upload capture is released once its commit is durable. A superseded
  local version is released once the reconciliation that replaced it is
  durable and no conflict refers to it.
- Conflict material stays until an explicit user decision. It is the only
  unbounded retention, and status reports it.
- Everything else is limited by a documented budget of count and bytes, pruned
  oldest first, and reported in status.
- Status and recovery do not rehash retained material on every run. An
  artifact is verified when it is created and again only when it is exported
  or used.

## Durability boundaries are explicit

- `write`, `append` and `truncate` acknowledge acceptance into a local
  writeback cache. A crash before the next barrier may lose that data.
- `flush`, `fsync`, `close` and an explicit commit are durability barriers.
  They make local state durable, then publish, and success means remote
  durability.
- A frozen request, its CommitId and its prepared content descriptors are
  locally durable before dispatch. The record that a request may have been
  dispatched is also durable before dispatch, so a crash can never turn an
  Unknown outcome into a safe abort.
- A journal record that crash recovery needs to interpret a local filesystem
  change, such as an install, exchange or rename, is durable before that
  change.
- Stores whose acknowledgements precede a barrier run WAL with
  `synchronous=NORMAL` and an explicit barrier. Journals whose every record
  precedes an effect commit with `synchronous=FULL`. Neither kind pays a
  per-write fsync for a guarantee the contract does not make.

## Untrusted bytes are verified once

- Bytes are verified where they cross a trust boundary: content prepared from
  a local source, content imported from an untrusted descriptor, and content
  read back from remote storage for a user.
- Content prepared by this process is not imported again before this process
  publishes it. Recovery after a restart verifies recorded content at most
  once.
- A remote read-back needs a stated integrity requirement and reads each byte
  at most once.

## Caches are content-addressed and staleness is explicit

- Caches of immutable, content-addressed data, such as index pages, values and
  content units, belong to the authority, are shared across snapshots, and
  have a byte bound.
- Metadata freshness is one documented, bounded staleness window, applied the
  same way by every frontend of a model. A frontend does not observe the
  latest revision on every metadata callback.

## Errors are typed

- Every failure carries an `ErrorKind`. Callers branch on kinds, never on
  message text.
- Library APIs do not return boxed `dyn Error`. The CLI converts typed errors
  into exit codes in one place.
- All frontends map kinds to platform errors through one shared table, so the
  same failure produces the same errno everywhere.

## Configuration and validation live in one place

- Every CLI entry point loads a volume through one loader. The loader resolves
  relative paths, applies defaults and runs admission before any side effect.
- Admission is one table over model, access, frontend and publication. The
  reported capabilities come from that table; there is no second negotiation
  path.
- One containment check rejects a root inside its state, a state inside its
  root, and a root and state on different filesystems. Init, open, backup,
  restore and every Sync model call it.

## No machinery without a caller

- A field, variant, trait method, profile or command exists only while a
  supported code path uses it. Remove it with its last caller.
- Before the first release YinYang has no compatibility obligation for local
  state, wire formats, JSON documents, CLI flags or Rust APIs. Do not add
  migrations, legacy readers, cross-version fixtures or version dispatch.
  Change the profile identifier and reject the old one.

## Specifications, tests and evidence

- `specs/` describes current behavior and changes in the same pull request as
  the behavior. A stale count, status line or capability claim is a defect.
- Measurements that support a pull request belong in its description or an
  external evidence record. `docs/` holds operator guides, not raw benchmark
  output or one-time qualification transcripts.
- Integration tests share fixtures through one support module, not through
  copies included by `#[path]` into every test binary.

## Review checklist

Every pull request description answers these questions:

1. Does the change add a second implementation of a concept listed above?
2. Does any new cost grow with history, retained state or tree size?
3. What releases each new piece of retained local state, and what bounds it?
4. Where is the durability barrier for each new acknowledgement?
5. Are any bytes verified or downloaded twice?
6. Does any caller branch on error text?
7. Which specification changed, and does it still match the code?
