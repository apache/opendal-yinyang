# Directory publication and restoration

The `yinyang` crate provides one-shot directory transfer over the experimental
object-storage `yinyang_core::Fs`. The runtime and CLI target native Linux,
macOS, and Windows, not WebAssembly. License checks cover their native target
graphs without enabling browser-only certificate bundles. There is no Mount,
background synchronization, offline reconciliation, or bidirectional Sync.

## Publication

Transfer APIs reject snapshots from another filesystem before local IO.
`prepare_directory(fs, snapshot, source, commit_id)` returns a frozen
`Transaction`; `publish_directory` is its one-shot prepare-and-commit
convenience. Retain the frozen transaction for exact `Fs::commit` retries.
Replanning from another snapshot or changed local input is a new logical
request and requires a new identity.

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

## Restoration

`restore_directory(fs, snapshot, destination)` restores exactly that pinned
revision, including after reopening the same filesystem. The destination must
not exist and its parent must exist. An existing destination or symlink is
rejected. After destination creation, prevent concurrent local mutation.

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
- `yy publish SOURCE [--replace] [--commit-id UUID]`: prepare and publish a
  complete replacement. Nonempty remote namespaces require `--replace`.
  An optional UUID identifies a **new** request, not a reconstructed retry.
  A previously committed identity is rejected with a pointer to receipt lookup.
- `yy receipt UUID`: query the current indexed receipt, displaying its revision,
  ordinal and original request digest. It does not read the source or publish
  anything. Absence exits nonzero and does not establish failure of a delayed
  attempt.
- `yy restore DESTINATION`: restore the pinned current snapshot into a new
  directory. Later publications do not change that restore.
- `yy status`: enumerate the pinned namespace and print revision, file count,
  and directory count including root. This is remote state, not sync status.

Revisions are printed as 24-byte hexadecimal tokens, not commit counts.
Conflict, Retryable, and Unknown are distinct nonzero outcomes. The CLI does
not persist frozen plans across process restarts or automatically replan
uncertain requests. Applications needing exact retry retain `Transaction`
and call `Fs::commit` with it; the CLI's receipt command is resolution-only.

## Backend acceptance

Deterministic faults cover source mutation, failed preparation, stale broad
scope, frozen-plan retries, corruption, pinned restore and no-clobber local IO.
The S3 CI job starts a pinned isolated MinIO instance and runs
`cargo test --workspace --all-targets s3_ -- --ignored --nocapture`.
It exercises multipart content, range overwrite, indexed publication, receipt
lookup and separate CLI processes for create, publish, status, replacement and
restore. These results apply to the tested binding, not arbitrary compatible
endpoints.
