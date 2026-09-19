# Directory publication and restoration

The `yinyang` crate provides one-shot directory transfer over `yinyang_core::Fs`.
The directory runtime and CLI target native Linux, macOS, and Windows builds,
not WebAssembly. Dependency license checks cover their GNU/musl, Apple, and
MSVC target graphs without enabling browser-only certificate bundles.
It does not implement Mount, a local state database, rename detection,
incremental change cursors, offline reconciliation, or bidirectional Sync.

## Publication

Both transfer APIs reject observations from another filesystem before local IO.

`publish_directory(fs, observation, source, commit_id)` treats the source as a
complete replacement namespace. It scans directories and regular files, rejects
symlinks, special files, non-UTF-8 names and non-portable paths, and validates
namespace changes before uploading. It never modifies the source. The source
must remain quiescent until completion: before/after metadata and rescans catch
common mutations but do not provide an atomic snapshot or protection against a
concurrent local adversary. Use a native filesystem snapshot if that is needed.

Files and directories at the same path retain their identities when their kind
is unchanged. File-to-directory and directory-to-file replacement creates a new
identity. Remote-only entries are removed. A path change is deletion plus
creation, not rename detection. Existing file contents are hashed locally;
unchanged bytes reuse their immutable representation. Changed or new contents
are uploaded before one checked tree commit. A scan, upload, or validation
failure leaves the remote head unchanged, but may leave unreachable uploads.

The original observation supplies the expected head. A competing publication
returns `Conflict` and is not automatically rebased. Callers retain the commit
ID across uncertain retries. If that ID is already present in the supplied
observation, publication returns its committed version without reading the
local directory again. A no-op snapshot may still create a commit while keeping
node and directory generations unchanged.

## Restoration

`restore_directory(fs, observation, destination)` restores exactly the supplied
immutable observation, even if another client publishes a newer version. The
destination must not exist; its parent must already exist. An existing
destination, including a symlink, is rejected rather than traversed or replaced. After the
destination is created, callers must prevent concurrent local mutation.

Each file is streamed into a temporary file in its destination directory. The
complete logical content and referenced blobs are verified, output is flushed
and synced, and the file is installed with no replacement. An IO or integrity
error prevents that file from being installed. An ordinary failure cleans up
its temporary file; directories and already verified files can remain. A crash
may leave temporary files. Restoration is not an atomic or crash-durable
whole-directory transaction and does not merge with an existing local tree.
Retry into a new destination after inspecting any partial output.

## Metadata and limits

The transfer preserves empty files and directories, file bytes, and the file
executable flag on Unix. Created Unix files are owner-readable/writable, with
owner execute permission when executable. Full modes, ownership, timestamps,
ACLs, xattrs, symlinks, sparse allocation, and hard-link identity are not
preserved. Hard-linked regular files are transferred independently. Non-Unix
sources have no executable flag; restoring an executable file on non-Unix or
an executable directory is rejected before creating the destination.

File IO uses bounded buffers. Directory metadata remains materialized and is
subject to the core's current version-size limit; this is not a million-file
scalability claim. There is no garbage collector. Do not delete objects under
the Managed prefix, including objects reachable through older observations.

## CLI

The `yy` binary currently enables the OpenDAL S3 service. Configuration comes
from `YINYANG_S3_*` variables; the suffix is the lower-case OpenDAL configuration
key. Common keys are `BUCKET`, `ROOT`, `REGION`, `ENDPOINT`, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY`, and `SESSION_TOKEN`. OpenDAL's AWS credential chain also
applies. Credentials should not be passed as command-line arguments. The
operator root identifies one filesystem and the bucket must already exist.

- `yy create`: create the filesystem or validate and reopen an existing one.
- `yy publish SOURCE [--replace] [--commit-id UUID]`: publish a whole directory.
  A non-empty remote namespace requires `--replace`, except when retrying an
  already committed ID. The commit UUID is printed before transfer.
- `yy restore DESTINATION`: restore the current observed version into a new
  directory. Later remote publications do not change that restore.
- `yy status`: print current version, file count, and directory count including
  the root. This reports remote state, not synchronization status.

Errors, including publication conflicts, exit nonzero. Successful publication
prints the committed version and ID. The CLI does not create buckets, delete
remote objects, retry conflicts automatically, or provide background work.

## Backend acceptance

Ordinary repository tests cover transfer through the same public core and
directory APIs with deterministic storage faults. The S3 CI job starts an
isolated, pinned MinIO instance and runs `cargo test --workspace --all-targets
s3_ -- --ignored --nocapture`. This exercises multipart file publication,
conditional head updates, a fresh core instance, and independent `yy`
processes for create, publish, status, retry, replacement, and restore. These
tests validate this S3-compatible implementation; other providers still need
their own capability and behavior checks.
