# Durable Sync intents

**Status: experimental content and namespace coordinator, independent of Mount.**

`yinyang::sync::Sync` accepts content replacement and namespace operations
against an explicit common remote baseline. It never substitutes a fresh
observation after an offline edit. Downloads use `Snapshot::open_file` and its
immutable verified ranges; enumeration uses the same Snapshot APIs. Ordinary
local directories and OS-managed provider trees supply their own platform
callbacks, local edit tracking and materialization.

## Content edits

`stage` streams replacement bytes into durable FileHandle staging, in 64 KiB
chunks. It currently materializes the baseline before replacing it. The returned
EditId is BLAKE3 with derive-key context `yinyang-sync-content-edit-1` over
filesystem ID, node ID, 24-byte baseline revision and all replacement bytes.
Repeating this exact intent returns the same local edit, including after
restart. An empty replacement is still conditional even when the baseline was
also empty.

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

## Namespace intents

The namespace API accepts only operations expressible by the shared
Authority/Planner contract:

```text
stage_create_file(parent, name, baseline, FileMetadata, bytes)
stage_create_directory(parent, name, baseline)
stage_remove(node, baseline)
stage_rename(node, baseline, new_parent, new_name, RenameMode)
stage_set_executable(node, baseline, executable)
```

`RenameMode` is explicitly `NoReplace` or `Replace`. `NoReplace` rejects an
occupied destination. `Replace` removes exactly the destination observed in
the same baseline snapshot and requires the same kind; a directory destination
must be empty. The source keeps its NodeId. Existing pinned versions and staged
handles of the replaced node remain readable but cannot publish back to the
removed identity.

Creation allocates its stable NodeId in the same Planner that produces the
persisted transaction. The intent exposes that NodeId; callers must not resolve
the path again and adopt a concurrent replacement. `CreateFile` also exposes
the prepared `ContentDescriptor`, allowing a local adapter to compare its
current bytes without a fresh remote lookup.

`FileMetadata` contains only the executable bit. File creation and subsequent
`SetExecutable` support that bit. Directory modes, ownership, ACLs, timestamps,
and other POSIX metadata are outside this contract; adapters must report them
unsupported before creating an intent. There is no recursive removal. Removing
a directory requires the observed empty directory and conflicts with a
concurrent child creation.

Every namespace intent is planned against `Authority::observe_revision` for its
supplied baseline and persisted with `Transaction::to_bytes` before its first
commit. Publication restores that exact transaction through the selected
Authority. It does not call current-head Runtime namespace helpers, rerun a
callback, choose a newer baseline, or allocate a different NodeId.

The Planner guards each selected name, parent kind, source link, destination,
and relevant ancestry. Rename additionally guards the complete logical state of
the source and of an observed replacement target. Remove guards the complete
source node, its link and directory membership. These dependencies distinguish
destructive content or executable changes from a safe namespace-only move.
The required interleavings are:

- Concurrent creates of one name publish at most one intent; the other is
  `Conflict`.
- A delete or rename conflicts when a content/executable edit against its
  baseline publishes first. If the namespace operation publishes first, a later
  content edit conflicts because its original node state is no longer live.
- Concurrent renames of one source publish at most one move.
- Child creation and removal of its observed empty parent publish at most one
  operation.
- Replacing a destination conflicts with a concurrent edit, replacement, or new
  child of that destination.

## Durable state and resolution

The dedicated directory contains `sync.db` (SQLite WAL, `synchronous=FULL`) and
`content/` (the existing runtime's durable staging and exclusive process lock).
Profile `yinyang-sync-2` binds the journal to one filesystem. Opening a valid
`yinyang-sync-1` journal upgrades it in place without dropping content edit
records. A mismatched filesystem or unknown profile is an error. Keep both
databases and their WALs together.

The content edit table maps 32-byte EditIds to 16-byte staged handle IDs. A
mapping is committed only after all local bytes are accepted; publication is
forbidden before that mapping. Interrupted staging can leave unreferenced,
never-dispatched content, collected on restart or before the next stage.
Missing referenced content is an error, not permission to replan.

The namespace table stores the logical operation, exact `CommitId`, canonical
request digest, baseline, transaction bytes, durable state, receipt, and typed
failure. One `EditId` identifies either a content edit or namespace intent.
`namespace_status` and `namespace_intents` expose the retained logical intent;
`edit_status` distinguishes the two edit families. Namespace state is:

| State | Meaning |
| --- | --- |
| Prepared | Accepted locally and durable, with no dispatch recorded. |
| Unknown | A dispatch may have published; receipt absence is not failure. |
| Committed | The original receipt is attached. |
| Conflict | The baseline-bound predicates no longer hold. |
| Retryable | The attempt is known not to have published. |
| Failed | Validation, readiness, corruption, or another definitive failure. |

Before dispatch, Sync changes `Prepared`/`Retryable` to `Unknown` in a durable
SQLite transaction. A crash or lost response therefore cannot turn a possibly
published request into a safe abort. `publish` retries `Unknown` and `Retryable`
with the same transaction and CommitId. It does not dispatch `Conflict` or
`Failed` again, and never replans either one. A caller that wants a different
intent must create a new baseline-bound EditId.

`resolve_namespace` checks only the original CommitId's receipt and verifies its
request digest before attaching it. It never dispatches. Returning no receipt
does not prove that a delayed attempt failed. Conflict resolution is explicit:
keep or export the relevant local and pinned baseline bytes, then create a new
intent with suitable current predicates.

Operations within one Sync instance serialize to protect intent creation,
staging leases, and state transitions. This is not a background worker,
directory reconciler, OS cache, or cross-process concurrent uploader. It does
not implement the ordinary-directory scanner, materialization policy, CLI,
File Provider callbacks, or a macOS frontend.

## Validation

Tests cover both publication authorities, repeated intent, empty replacement
conflicts, pinned downloads, retained conflicting bytes, and Unknown recovery
after restart. Namespace tests cover create file/directory, stable-identity
rename, no-replace and replace, executable metadata, create/create,
delete/edit, rename/edit, rename/rename, parent removal, and response-loss
replay after process reopen.
