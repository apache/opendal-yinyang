# Conditional Sync edits

**Status: experimental content coordinator, independent of Mount.**

`yinyang::sync::Sync` accepts full-file replacement bytes against an explicit
common remote baseline (NodeId and retained Revision). It never substitutes a
fresh observation after an offline edit. Downloads use `Snapshot::open_file`
and its immutable verified ranges; enumeration uses the same Snapshot APIs.
Ordinary local directories and OS-managed provider trees supply their own
platform callbacks, local edit tracking and materialization.

`stage` streams bytes into durable FileHandle staging, in 64 KiB chunks. It
currently materializes the baseline before replacing it. The returned EditId is
BLAKE3 with derive-key context `yinyang-sync-content-edit-1` over filesystem ID,
node ID, 24-byte baseline revision and all replacement bytes. Repeating this
exact intent returns the same local edit, including after restart. An empty
replacement is still conditional even when the baseline was also empty.

`publish` uses that edit's original staged baseline and frozen request. Remote
success returns the revision that accepted the edit, never a later observation.
Conflicts retain local bytes for export with `read`; Unknown/Retryable retain the
exact request. Completed edits stay available for idempotent retry. The current
experimental profile retains these records indefinitely; it has no automatic
garbage collection or conflict-resolution policy.

Local staging, remote completion and platform acknowledgment are separate.
An adapter may mark a file in-sync only if the currently observed local version
still matches the completed edit. Completion of v1 must not acknowledge a newer
v2. In particular local fsync inside a File Provider domain is not remote fsync.

The dedicated directory contains `sync.db` (SQLite WAL, synchronous FULL) and
`content/` (the existing runtime's durable staging and exclusive process lock).
Profile `yinyang-sync-1` binds the journal to one filesystem. The journal maps
32-byte EditIds to 16-byte staged handle IDs. A mapping is committed only after
all local bytes are accepted; publication is forbidden before that mapping.
Interrupted staging can leave unreferenced, never-dispatched content, collected
on restart or before the next stage. Missing referenced content is an error,
not permission to replan. Keep both databases and their WALs together.

Operations within one Sync instance serialize to protect intent creation and
staging leases. This is not a background worker, directory reconciler, OS cache,
or cross-process concurrent uploader. It does not yet coordinate namespace edits.
Tests cover both authorities, repeated intent, empty replacement conflicts,
pinned downloads, retained conflicting bytes, and Unknown recovery after restart.
