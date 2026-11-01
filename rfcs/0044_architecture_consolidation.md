- Proposal Name: `architecture_consolidation`
- Start Date: 2026-11-01
- RFC PR: [apache/opendal-yinyang#TBD](https://github.com/apache/opendal-yinyang/pulls)
- Tracking Issue: [apache/opendal-yinyang#67](https://github.com/apache/opendal-yinyang/issues/67)

# Summary

Consolidate YinYang around one shared implementation per concept, make local
costs proportional to the change being made, bound every piece of retained
local state, and move write durability to explicit barriers. Record the rules
in [`specs/design-principles.md`](../specs/design-principles.md) so later work
does not reintroduce the same problems.

# Motivation

An architecture review of `main` at `3d1c0b2` on 2026-11-01 measured these
problems with the in-memory test backend and 512 KiB files:

- **Unbounded retained state.** Managed Sync kept a full copy of every
  published and pulled file, plus one copy per remote modification, with no
  release path. 256 files kept 128 MiB on each side. After 20 edits of one
  1 MiB file the writer kept 21 MiB and the reader 41 MiB while reporting
  `in_sync`. Every run rehashed the retained copies: a no-op run read 2.1, 12.1
  and 42.1 MiB after 0, 10 and 40 edits.
- **Quadratic pull.** Pull rescanned and rehashed the whole local tree after
  each materialized entry. It took 3.9, 12.4, 44.6 and 151.6 seconds for 32,
  64, 128 and 256 files, while push took 2.4 to 18.6 seconds. One pull of 128
  files read 68.5 times the tree size.
- **Per-file commits and repeated reads.** Pushing N files issued about N+8
  head writes although `commit_batch` existed. Publication re-imported every
  file it had just prepared, reading a new 4 MiB file back once on the object
  authority and twice through the service.
- **Per-write fsync.** Every staged write was its own `synchronous=FULL`
  SQLite transaction: 1.6 MiB/s for 4 KiB writes and 41 MiB/s for 128 KiB
  writes, against 12.4 and 205 MiB/s with `synchronous=NORMAL`. RFC-0016 only
  requires that `write` reach a local writeback cache and that `fsync` reach
  remote storage.
- **Uncached metadata.** Managed FUSE observed the latest revision on every
  LOOKUP and GETATTR with a cold per-snapshot index cache: 9 to 10 storage
  reads per lookup in a 1000-entry directory. Direct FUSE instead served a
  cached snapshot, so the two frontends had different freshness semantics.
- **Inconsistent validation.** A Managed Sync configuration without `staging`
  passed `sync` admission and placed its state beside the configuration file,
  while other commands rejected the same file. Replica init rejected only one
  containment direction, so `sync backup` succeeded on a layout that `sync
  restore` then refused.
- **Forked implementations.** Managed and Direct FUSE shared about 430
  duplicated lines and already disagreed on conflict errno. Managed Sync
  (10,000 lines) and Direct Sync (5,300 lines) had separate local trees,
  install journals, artifact stores, schemas, evidence types and status
  documents. At least five intent state machines, nine SQLite profiles, five
  error types, six storage operator constructors and a capability negotiation
  layer that duplicated admission existed side by side, along with migrations
  and about 5,900 lines of cross-version tests for formats that were never
  released.

RFC-0016 describes a shared filesystem core under every frontend. The
implementation instead grew one fork per model, access mode and frontend, and
delivered breadth before the costs that make each combination usable.

# Decisions

The maintainer made four decisions on 2026-11-01:

1. **Retained artifacts are released automatically and otherwise bounded.**
   Upload captures and superseded versions are released when the operation
   that needed them is durable. Remaining safety copies have a count and byte
   budget. Only conflict material waits for an explicit decision.
2. **Writes are durable only at `flush` and `fsync`.** `write`, `append` and
   `truncate` may be lost by a crash before the next barrier. Frozen requests
   remain durable before dispatch.
3. **Every product combination stays.** Managed Mount, Managed Sync, Direct
   Mount, Direct Sync, both Managed authorities, one-shot directory transfer,
   space and retention reporting, and the native bridge remain maintained.
   Consolidation replaces duplicated implementations; it removes no product.
4. **No compatibility is required.** Local state profiles, wire formats, JSON
   documents, CLI flags and Rust APIs may change without migration. Existing
   migrations, legacy readers and cross-version fixtures are deleted.

# Design

The rules in [`specs/design-principles.md`](../specs/design-principles.md)
define the target. The work lands as independent pull requests:

- **Local state.** One SQLite helper owns open, schema validation and named
  row encoding. Stores use WAL with `synchronous=NORMAL`; barriers make state
  durable at `flush`, `fsync`, `close`, commit and before dispatch. Migrations,
  legacy profiles, cross-version tests and their fixtures are removed.
- **Configuration.** One loader resolves paths and runs admission for every
  command. One containment check serves init, open, backup, restore and both
  Sync models. Admission and reported capabilities come from one table. Errors
  are typed through the CLI and `doctor`.
- **FUSE.** One adapter serves Managed and Direct through a backend trait.
  Managed metadata reuses an accepted snapshot within a bounded staleness
  window, and the object authority shares a bounded content-addressed page
  cache across snapshots.
- **Sync.** Managed Sync releases upload captures after commit, bounds other
  retained copies, compares content without copying it, tracks per-path
  fingerprints, publishes one batch per pass, and skips re-importing content
  it prepared. Managed and Direct Sync then share one engine with one local
  tree, artifact store, schema, status document and backup path over a remote
  adapter.
- **Core and runtimes.** Authority forwarding defaults, the duplicated backend
  profile enum, unused capability machinery and per-call service connections
  are removed. Sync content edits stop round-tripping through file-handle
  staging.
- **Documentation and tests.** Stale status claims are corrected, raw
  benchmark output leaves `docs/`, integration fixtures share one support
  module, and growth regressions with realistic sizes guard the costs above.

# Contracts that remain

Consolidation keeps these guarantees unchanged:

- Unknown outcomes stay explicit and replay their original CommitId and frozen
  request.
- A frozen request is durable before dispatch.
- No silent overwrite and no last-writer-wins resolution.
- Conflicts are retained until an explicit decision.
- Local installs stay fd-relative and race-checked.
- Admission completes before any side effect.
- Mount `fsync` succeeds only after remote publication.

# Compatibility

There is no compatibility promise before the first release. Local state from
earlier builds, the stage-1 and stage-2 staging profiles, the sync-1 profile
and earlier JSON documents are rejected rather than migrated. Operators must
finish or export pending work with the old binary before upgrading.

# Unresolved Questions

- The default artifact budget may need tuning once real workloads report
  their retained sizes.
- The metadata staleness window may become configurable if an application
  needs fresher LOOKUP results than the default.
