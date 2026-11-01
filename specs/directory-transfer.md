# Directory publication and restoration

The `yinyang` crate provides one-shot directory transfer over the experimental
object-storage `yinyang_core::Fs`. The runtime and CLI target native Linux,
macOS, and Windows, not WebAssembly. License checks cover their native target
graphs without enabling browser-only certificate bundles. This specification
covers only the finite, caller-driven publication and restoration API: it does
not define the Managed mount frontend ([Linux FUSE frontend](linux-fuse.md)),
the durable file-operation runtime ([File-operation runtime](file-runtime.md)),
or baseline-bound reconciliation ([Sync runtime](sync-runtime.md)). Those
frontends and runtimes own their own contracts.

## Publication

Transfer APIs reject snapshots from another filesystem before local IO.
`prepare_directory(fs, snapshot, source, commit_id)` returns a frozen
`Transaction`; `publish_directory` is its one-shot prepare-and-commit
convenience. Retain the frozen transaction for exact `Fs::commit` retries.
Replanning from another snapshot or changed local input is a new logical
request and requires a new identity.

`DirectoryJournal` adds opt-in cross-process recovery for callers that cannot
retain all in-process evidence. `prepare` accepts only a new identity and rejects
an identity already in the journal before reading the source. `publish` takes
only a `CommitId`; it restores and retries the original frozen transaction and
never accepts, scans or replans from a source path.

For backup, stop the publishing process and use a consistent SQLite backup of
`directory.db`; copy the complete immutable object prefix referenced by the
journal. Restoring the journal plus that prefix preserves receipt lookup,
source-free retry, export, and historical restore. See
[Linux backup and isolated recovery](../docs/linux-backup-recovery.md).

The source is a complete replacement namespace. The planner scans all relevant
remote nodes and directory memberships, so concurrent content changes and
unseen additions within that scope cause Conflict. It does not silently erase
concurrent changes. Core CAS retries still replay this same prepared plan;
physical head contention is not itself a semantic conflict.

Local scanning rejects symlinks, special files, non-UTF-8 names, non-portable
components and case-folded sibling collisions before upload. It never changes
the source. Keep the source quiescent until preparation completes: fingerprints
and rescans detect common changes but do not provide an atomic local snapshot
or protection against a concurrent local adversary. Use a native filesystem
snapshot when that guarantee is needed.

Same-path, same-kind nodes retain identities. Kind changes create new identities.
Remote-only entries are removed; path changes are deletion plus creation, not
rename detection. Existing bytes are identified locally with the canonical
Merkle profile. Unchanged content reuses the published representation without
uploading; changed/new content is prepared and verified before the frozen plan
is returned. A preparation failure leaves the remote head unchanged and may
leave unreferenced immutable objects.

The request binds all original logical predicates and mutations, including
logical content identities. Reusing a committed ID for different intent is
invalid. A known no-op may still commit a receipt without advancing logical
generations. Receipt lookup alone is not publication of a newly scanned source.

## Durable publication recovery

The caller supplies a dedicated local state directory. It contains
`directory.db`, an experimental SQLite WAL database using
`synchronous=NORMAL`, bound to exactly one filesystem and root. Keep the
database and WAL together. Profile `yinyang-directory-2` stores the exact
serialized `Transaction`, its
CommitId, baseline revision, request digest, diagnostic source path, and a
directory manifest derived during that same successful preparation. Unknown or
mismatched profiles and filesystem bindings are rejected. This local format has
no migration path.

The supported schema signature is the exact table set `binding` and
`publications` with their exact column-name sets. Maintained inserts name their
columns explicitly, so column order is not part of the signature. A writer
validates an existing database before journal-mode configuration or DDL. A
missing or zero-table database may be initialized after the journal's SQLite
write transaction starts; an older or unknown profile returns `Unsupported`,
and a current profile with a missing or extra column is `Corrupt`. Read-only
inspect performs the same profile and schema validation without creating
tables, changing authoritative database/WAL bytes, or changing journal mode.
SQLite may create an empty `-wal` and may create or update derived `-shm`;
existing WAL/journal bytes remain protected.

The frozen publication row is checkpointed before the function returns.
`Prepared` is therefore durable before dispatch, and the `Unknown` claim is
checkpointed before every commit request.

The canonical journal directory must be outside the canonical source tree. A
preparation that would publish the journal's own `directory.db` or WAL files is
rejected after the existing-identity check but before local scanning or content
upload. Path aliases and symlinked source roots are resolved before this check.
A source located below a larger state directory is allowed when the journal
files themselves remain outside that source.

The manifest includes the root, empty directories, unchanged files, prepared
files and executable flags. It exists to project paths for status and export; it
does not replace the frozen transaction as publication authority. Export never
rescans the source and never substitutes a newer transaction for the accepted
manifest.

Publication state is reopen-safe:

| State | Meaning |
| --- | --- |
| Prepared | The transaction, manifest and content readiness are durable; no dispatch has begun. |
| Unknown | A dispatch may have published. Receipt absence is not proof of failure. |
| Committed | The original CommitId and request digest are attached to a receipt. |
| Conflict | Original namespace predicates changed; the frozen request is terminal. |
| Retryable | The authority established that the attempt did not publish. |
| Failed | Local validation or retained readiness is definitively unusable. |

Before first dispatch, the journal synchronously changes the record to
`Unknown`. A crash or lost response therefore cannot turn a possibly published
request into a safe abort. On retry, `Unknown` first checks the original
CommitId's receipt. If no receipt exists, the authority restores readiness and
the original predicates from the retained transaction before the identical
request is dispatched.

Once `Unknown` is durable, restore, readiness, network, and local receipt-
persistence failures do not prove nonpublication. They retain `Unknown` and may
replace the diagnostic; only definitive authority evidence moves the record to
Committed, Conflict, or Retryable. A changed local source cannot affect retry
and must use a new identity for new intent.

`DirectoryJournal::inspect` opens `directory.db` read-only and returns intents in
CommitId order with original identity, baseline, digest, state, receipt and
typed failure. It requires no `Fs`, remote credentials, network or writer lease.
`DirectoryJournal::export(commit_id, destination)` requires the authority: it
reads the prepared immutable content referenced by the frozen plan through
authenticated content verification. It remains usable after source mutation or
process restart, but it is not an offline export.

## Restoration

`restore_directory(fs, snapshot, destination)` restores exactly that pinned
revision, including after reopening the same filesystem.
`restore_directory_revision(fs, revision, destination)` resolves one retained
historical revision before applying the same rules. The destination must not
exist and its parent must exist. An existing destination or symlink is rejected.
After destination creation, prevent concurrent local mutation.

Each file streams through authenticated verification units into a temporary
file in its destination directory. After the full read, output is flushed and
synced, then installed without replacement. An IO or integrity error prevents
that file's installation. Ordinary errors clean up its temporary file;
directories and previously verified files may remain. A crash may leave
temporary files. Restoration is not an atomic or crash-durable whole-directory
transaction. Retry into a new destination after inspecting partial output.

## Metadata and costs

Empty files/directories, file bytes and Unix executable flags are preserved.
Unix files are owner-readable/writable, plus owner execute when requested.
Full modes, ownership, timestamps, ACLs, xattrs, symlinks, sparse allocation,
and hard-link identity are not preserved. Hard-linked files transfer
independently. Non-Unix sources have no executable flag. Executable files on
non-Unix and executable directories are rejected before destination creation.

File buffers are bounded. Complete directory transfer materializes the names
and local metadata that its broad replacement semantics require; core
transactions, lookup and pagination do not materialize the filesystem.
There is no stored 4096-byte full-path limit or materialized-version size limit.
The object profile's per-value resource limit still bounds one encoded change
record. No garbage collection or retention expiry is implemented.

## CLI

The `yy` binary enables OpenDAL's S3 service. Configuration comes from
`YINYANG_S3_*`; suffixes are lower-case OpenDAL configuration keys. Common keys
are `BUCKET`, `ROOT`, `REGION`, `ENDPOINT`, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY`, and `SESSION_TOKEN`. The AWS credential chain also
applies. Do not put credentials in command arguments. The operator root owns
one filesystem; the bucket must already exist.

`YINYANG_STORAGE_PROFILE` is `amazon-s3` or `minio`. With no custom endpoint,
Amazon S3 is the default. A custom endpoint requires an explicit profile and
must provide its documented semantics. Other S3-compatible deployments are
not automatically supported.

- `yy create`: create or validate/reopen the object-storage filesystem.
- `yy publish SOURCE [--replace] [--commit-id UUID] [--state DIR]`: prepare and
  publish a complete replacement. Nonempty remote namespaces require
  `--replace`. An optional UUID identifies a **new** request. If the journal
  already contains that identity, the command rejects the supplied source with
  a retry instruction rather than silently ignoring it.
- `yy receipt UUID`: query the current indexed receipt, displaying its revision,
  ordinal and original request digest. It does not read the source or publish
  anything. Absence exits nonzero and does not establish failure of a delayed
  attempt.
- `yy publication inspect [ID] --state DIR [--json]`: inspect one or all local
  intents without remote access.
- `yy publication retry ID --state DIR`: exact source-free replay of one
  existing request.
- `yy publication export ID --state DIR --destination DIR`: export the accepted
  preparation using retained immutable content.
- `yy restore DESTINATION [--revision REV]`: restore the current or selected
  retained revision into a new directory. Later publications do not change that
  restore.
- `yy status [--revision REV]`: enumerate the current or selected retained
  namespace and print revision, file count, and directory count including root.
  This is remote state, not sync status.

Revisions are printed as 24-byte hexadecimal tokens, not commit counts.
Conflict, Retryable, and Unknown are distinct nonzero outcomes. Unknown uses
exit code 3; Conflict and Retryable use exit code 4; other failures use exit code
1. The CLI never replans an uncertain request and never treats an absent receipt
as proof that publication failed.

## Backend acceptance

Deterministic faults cover source mutation, failed preparation, stale broad
scope, frozen-plan retries, lost publication responses, process reopen,
corruption, historical restore and no-clobber local IO.
The S3 CI job starts a pinned isolated MinIO instance and runs
`cargo test --workspace --all-targets s3_ -- --ignored --nocapture`.
It exercises multipart content, range overwrite, indexed publication, receipt
lookup, durable inspect/retry/export and separate CLI processes for create,
publish, status, replacement, historical restore and recovery. These results
apply to the tested binding, not arbitrary compatible endpoints.
