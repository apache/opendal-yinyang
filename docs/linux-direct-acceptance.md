# Linux Direct acceptance

`cargo x test-linux-direct` exercises deterministic read-only and writable
Direct Mount plus finite Direct Sync through the real `yy` CLI. Backend
acceptance compares the visible raw S3 inventory, exact object metadata and
bytes before and after each operation; a command exit alone is not evidence of
convergence or non-mutation.

The command requires Linux with `/dev/fuse` and `fusermount3`, isolated
plain-HTTP Silo `YINYANG_S3_*` settings, Python 3 for the xattr rejection probe,
and `YINYANG_DIRECT_VERSIONED_BUCKET` with versioning enabled. The deterministic
command explicitly excludes the credentials-gated `direct_amazon_s3_` cases.
Real Amazon S3 qualification remains a separate explicit run.

The harness sends its own path-style signed requests to seed and inspect raw
keys. Fixture changes made while a writable file is dirty use in-process
helpers; they do not launch subprocesses that could close an inherited FUSE
descriptor and accidentally trigger FLUSH. The controlled complete-list
collision fixture is the exception to backend LIST evidence: it verifies both
exact keys and deliberately supplies the complete protocol response because
the selected Silo endpoint omits one.

## Read-only Mount

- Direct JSON with `read_only` omitted and explicit `true`; explicit `false`
  without writable prerequisites is rejected before storage effects.
- `capabilities`, `doctor` and `status` without Managed metadata or staging.
- Complete-observation rejection for non-empty markers, overlong names and a
  controlled file-prefix collision.
- Reversible percent mapping with case and Unicode-normalization distinctions.
- Raw external replacement and deletion, retained descriptor attributes,
  completed `SIGHUP` invalidation and fresh observation.
- A true versioned bucket that retains an old descriptor's exact version and an
  unversioned ETag path that returns `ESTALE` rather than an unconditional read.
- SIGTERM teardown and fresh mount without storage mutation.

The recursive LIST must return every current key exactly. Pagination is not an
atomic snapshot. The selected Silo endpoint can GET both `a` and `a/b` while its
LIST exposes only `a`; Direct cannot discover the hidden key without guessing
names, so that namespace is outside the support boundary.

## Writable Mount

- Explicit `read_only: false` requires staging. The effective capabilities
  include whole-object write, append, truncate, remote fsync and recoverable
  staging, but omit Managed stable identity, atomic rename and retained unlink
  reads.
- Offset writes, truncate/extension and append publish exact bytes through
  final FLUSH/FSYNC. Remount reconstructs the accepted object.
- A read-only descriptor close cannot publish another descriptor's dirty bytes.
- Conditional create never clobbers an object that appears externally.
  Conditional replacement conflicts with an external ETag change and retains
  local conflict bytes through restart.
- The fault gate targets the final conditional request by method and key and
  verifies its `If-Match`. A request dropped before dispatch remains distinct
  from a response lost after a durable write; neither exit code nor a later
  observation is treated as an exact-once receipt.
- Create/append bytes acknowledged before crash survive daemon SIGKILL and
  restart. A newer local edit made after Unknown remains distinct and is not
  automatically replaced by an older frozen request.
- A clean cached key reopens a new externally replaced snapshot identity after
  SIGHUP; any retained old descriptor stays on its old pin or returns stale.
- Editor-style write-temp-then-rename is tested as a documented non-atomic
  safe-save path. A held source cleanup exposes destination-applied/source-
  retained partial state, and retry completes cleanup. No POSIX atomic rename
  capability is claimed.
- Unsupported chmod, chown, xattr, links, directory rename, exchange and
  whiteout operations fail without changing raw inventory.

Conditional mutation uses the observed ETag for compare-and-swap. A version ID
may identify retained bytes, but the suite does not claim version-only CAS; a
replacement with the same ETag has the backend's ABA limit. Direct mutation
produces storage observations, not Managed commit receipts.

## Direct Sync

- `sync init`, finite `sync run` and `sync status --json` use a Direct volume
  with `access=sync`, `frontend=library` and explicit staging.
- Local and remote additions, edits and deletions converge; status reports
  `in_sync=true` only when local scan, live remote observation and durable work
  agree.
- Concurrent local and remote edits produce an explicit conflict. Both conflict
  sides export exact bytes, and `resolve --keep local|remote` applies the chosen
  current state.
- Dropped requests and lost responses remain Unknown with retained input and
  local bytes. A later run cannot silently publish an older frozen request over
  a newer local edit; explicit current-state resolution publishes the observed
  desired bytes with the then-current ETag condition.
- Empty local directories and remote directory markers are outside the current
  Direct file namespace. They are rejected before reconciliation effects or
  reported as explicit non-sync failures, never silently omitted while status
  reports convergence.
- File-to-directory and directory-to-file changes produce conflicts or
  unsupported diagnostics while preserving both the remote objects and local
  observation.

The shared `YINYANG_S3_BUCKET` is not versioned by the suite. The separate
versioned bucket remains isolated from Managed suites.
