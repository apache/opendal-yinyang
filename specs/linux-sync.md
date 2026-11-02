# Linux ordinary-directory Sync

**Status: experimental.**

`yy --volume CONFIG sync` reconciles one ordinary Linux directory with one
Managed or Direct remote namespace. Both models use
`yinyang::sync_engine::SyncEngine`; the model is selected by the adapter built
from the admitted volume. The engine owns local scanning, the common baseline,
reconciliation, retained artifacts, conflicts, local installs, status and
backup. The adapter owns remote observation and conditional publication.

## Commands

```text
yy --volume CONFIG sync init    --directory ROOT
yy --volume CONFIG sync backup  --output BACKUP
yy --volume CONFIG sync restore --backup BACKUP --directory ROOT
yy --volume CONFIG sync run
yy --volume CONFIG sync status  [--json]
yy --volume CONFIG sync export  --artifact ARTIFACT_ID --output PATH
yy --volume CONFIG sync export  --conflict CONFLICT_ID --side local|remote --output PATH
yy --volume CONFIG sync resolve ID --keep local|remote
yy --volume CONFIG sync resolve ID --content PATH
```

`init` creates or validates the ordinary local root, validates the state
directory and binds both to the configured model and namespace. The root and
state must be distinct, have no containment relationship, and share one
filesystem. Initial admission requires exactly one side to contain files or
directories. Both non-empty sides fail rather than inventing a merge.

`run` performs one finite pass. `status` is read-only and remains useful while a
writer holds the state lease or the remote is unavailable. `export --artifact`
and the local side of a conflict are available offline. The remote conflict side
requires the configured remote.

`backup` writes one quiesced package containing the bound root,
`sync-engine.db`, adapter support state, retained artifacts and a
checksum-protected manifest. It refuses incomplete installs. `restore` validates
the complete package and adapter-specific support state before creating either
absent destination. Restore rebinds root, state, baseline and artifact inode
identities while preserving accepted content evidence, requests and receipts.

## Admission

The configured volume must use `access: "sync"`, `frontend: "library"`, and
`read_only: false`. Managed uses its configured object or loopback service
authority. Direct uses its storage-native namespace and requires the conditional
create, replace and delete capabilities from [direct.md](direct.md).

The `staging` field is the Sync state directory. It never contains credentials.
Shared volume admission validates configuration and storage before any state or
remote effect.

## Capabilities

An adapter reports a closed capability set. Each flag selects a real engine
path:

| Capability | Meaning |
| --- | --- |
| `directories` | The remote represents empty directories as namespace entries. |
| `executable` | The remote stores the executable bit. |
| `rename` | The remote supports identity-preserving namespace rename. |
| `batch` | Multiple frozen requests may be submitted in one adapter call. |
| `max_batch` | Maximum requests the adapter accepts in one call. |
| `delta` | Observation accepts the accepted entry map and a stored cursor. |
| `receipt_lookup` | A possibly dispatched request can be resolved by its original receipt. |

Managed reports directories, executable metadata, rename, batches, change-feed
deltas and receipt lookup. Direct reports none of those capabilities; it applies
one exact ETag-or-absent request at a time and always performs a full
observation.

## State profile

The shared profile is `yinyang-sync-engine-2`. Its exact tables are:

```text
binding
baseline
baseline_meta
artifacts
installs
conflicts
intents
work
```

`binding` records the model, namespace identity and digest, canonical root and
state paths, and root/state filesystem identities. `baseline` records the
accepted remote entry and accepted local fingerprint per path.
`baseline_meta.cursor` is the only persisted remote change cursor.

`intents` is the durable request log and uses the shared state codec documented
in [sync-runtime.md](sync-runtime.md). It stores the adapter request identity
and digest, opaque frozen request bytes, typed state, optional commit identity,
receipt, typed failure and engine work association metadata. A request is
marked `Unknown` in a durable transaction before dispatch can occur. `work`
stores only the path association and accepted inputs; it reads request state
from `intents`.

Managed namespace planning and content preparation are internal adapter work.
The engine sends mixed create and content-edit requests through one
`publish_batch`, so a Managed pass at one depth produces one authority head
commit.

SQLite uses WAL with `synchronous=FULL`. Older profiles and current profiles
with missing, extra or malformed tables/columns are rejected. There is no
migration.

## Reconciliation

The engine scans the local tree before planning and keeps that observation for
the pass; it observes a path again only where a local effect requires current
identity evidence. Persistent scan evidence is reused only while inode, size,
mtime and ctime match and the stat is racy-clean. Evidence never replaces the
accepted content fingerprint.

Planning is a three-way comparison of baseline, live local and live remote.
Parents are created before children and removals run children first. No remote
overwrite or last-writer-wins path exists. A file/directory kind conflict blocks
automatic changes below that path. A remote-deleted directory is quarantined as
one subtree only when every local descendant still matches the baseline;
otherwise its retained descendants become explicit conflicts.

File moves use stable remote identity only when the adapter supports rename.
Otherwise they converge as create followed by delete. A rename that races an
edit or another rename is retained as a conflict with source and desired paths.

Independent publishable files are frozen first and submitted through
`publish_batch` according to `batch` and `max_batch`. Managed creates at one
directory depth therefore contribute to one head commit. Direct applies the
same frozen requests one at a time. Content edits use the same publication path
as creates.

With `delta`, Managed starts from the accepted entry map and
`baseline_meta.cursor`. Changes are applied by stable NodeId, affected paths are
recomputed, and a renamed directory re-walks only its new subtree. An
unavailable or truncated cursor falls back to one full walk. Direct always
walks the remote namespace.

## Unknown, conflict and resolution

An adapter returns exactly `Applied`, `Conflict`, `Unknown`, `Retryable` or
`Unsupported` per frozen request. A batch member keeps its own outcome.

`Unknown` means dispatch may have occurred. The original frozen request and
retained input remain. Receipt-capable adapters look up the original receipt on
the next retry pass; otherwise they replay the same request identity with the
same source and precondition. A later observation never proves that an
ambiguous request committed. A newer local edit is not replaced by the older
accepted input.

Conflicts retain both available sides until an explicit decision. `--keep
local` publishes the current local decision against a newly observed remote
precondition. `--keep remote` materializes the observed remote state without a
publication. `--content PATH` prepares the selected bytes directly and records
that selected input, not the live file observed when a response arrives.

## Local install

The engine creates a private fd-relative temporary file and an `installs` row
before dispatch. The adapter writes remote bytes into the supplied borrowed fd
and verifies them: Managed verifies the core content identity; Direct verifies
its accepted version/ETag, size and read pin.

Installation then verifies the temporary through an open descriptor and applies
it by descriptor-relative rename or exchange. The expected local inode, parent
identity and bytes are checked again at the effect boundary. A changed target is
retained and becomes a conflict. Deletion first quarantines the exact observed
inode. Recovery interprets only durable install rows and never guesses from a
path name alone.

## Retained artifacts

The artifact kinds are `replaced-file`, `rollback-displaced`,
`quarantined-file`, `quarantined-directory`, `upload-retained`,
`conflict-local`, `conflict-remote` and `resolution`. Every row names its release
event.

Upload inputs are released after the accepted publication is durable. A
superseded local version survives one reconciliation boundary so a late write
through an already-open descriptor is observable. Conflict material remains
until explicit resolution. Other unreferenced artifacts are bounded to 64
entries and 256 MiB, oldest first.

A retained file changed after displacement is reported as a conflict from its
cheap stat evidence. Export reads the current retained bytes. Directory
quarantine is exported as a retained path; byte-stream export of a directory is
unsupported.

## Status

`status --json` always emits schema `yinyang.sync.status.v1`:

```text
{
  "schema": "yinyang.sync.status.v1",
  "model": "managed|direct",
  "binding": { "state": "unbound|bound|error", "state_directory": "...",
               "root": "...", "namespace": "...", "same_filesystem": true,
               "error": null },
  "remote": { "availability": "not_checked|available|unavailable",
              "cursor": "...", "error": null },
  "local": { "scan": "not_scanned|clean|changed|failed", "error": null },
  "summary": { "in_sync": false, "pending": 0, "unknown": 0, "conflicts": 0,
               "failed": 0, "retained_artifacts": 0, "retained_bytes": 0 },
  "entries": [],
  "conflicts": [],
  "artifacts": []
}
```

Each entry carries local, remote and baseline evidence, its current typed
intent/outcome and an optional conflict identity. A rename conflict references
the desired-path entry. `summary.conflicts` and
`summary.retained_artifacts` equal their top-level array lengths.

`summary.in_sync` is true only when the binding and remote are valid, the local
scan is clean, no pending/Unknown/failed/conflict work remains, every entry is
`in_sync`, and each baseline row pairs compatible local and remote content
evidence. A mismatch is a typed corrupt-state error, never `in_sync`.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | The finite command completed, or a valid bound status was emitted. |
| 3 | Status is unbound, or an operation retained an `Unknown` request. |
| 4 | Status is an invalid binding, or an operation ended `Conflict`/`Retryable`. |
| 1 | Argument, admission, configuration or unexpected operation failure. |

`run` may succeed with pending or conflict evidence. It fails when an Unknown
request remains unresolved.

## Cost acceptance

For both adapters, one pull hashes at most twice the pulled tree size, a settled
no-op hashes zero local bytes, and a settled state retains zero artifact bytes.
Managed creates at one depth use one adapter batch and therefore one authority
head commit. Direct uses one remote mutation per file, as required by its
non-batch capability. No-op cost is independent of process lifetime and tree
history.

## Limits

Sync is Linux-only and ordinary-directory-only. It has no watcher or daemon.
Direct does not support empty directory markers, executable metadata, atomic
rename or receipt lookup. Managed depends on the authority's retained change
cursor; an unavailable cursor falls back to a full walk.
