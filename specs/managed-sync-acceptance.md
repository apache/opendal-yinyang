# Managed Sync acceptance

This specification defines how real Linux acceptance proves Managed Sync against
an ordinary local directory. It is not evidence that macOS File Provider
callbacks or performance targets are complete.

## Required environment

Acceptance runs on Linux against the pinned `pgsty/silo` test image through
`YINYANG_S3_*`. The bucket and prefix must be isolated test resources. Missing
storage settings, credentials, the `yy` binary, or any required process tool
must fail the suite. A skipped or partially selected suite is not a pass.

The suite starts real `yy` processes. Object publication creates an independent
object authority; service publication starts a real `yy serve` process and
restarts it where a scenario requires process loss. Local directories and Sync
state are separate paths. Each client has its own Sync state and process
lifetime.

Remote observations used for assertions come from a newly constructed core
authority. The test does not inspect the adapter's cache, rely on its returned
remote tree, or reuse a successful materialization as authority evidence.

## Acceptance matrix

Both publication authorities run the behavior-sensitive scenarios:

| Area | Required evidence |
| --- | --- |
| Initial pull | A pre-existing remote tree materializes into an empty local root, including nested directories, empty files and supported executable bits. |
| Initial push | A non-empty local tree with empty remote state reaches remote with the same names, kinds and bytes. |
| Local namespace | Create, rename and remove of files and directories appear remotely, and an unchanged second run performs no semantic work. |
| Remote namespace | Create, rename and remove made through an independent core authority materialize locally without replacing unrelated local data. |
| Repeated convergence | Two successful finite synchronizations leave local and independently observed remote trees equal. |
| Offline restart | A local edit made with the authority unavailable survives process exit, authority restart and another explicit synchronization. |
| Local preservation | A remote change racing a local edit never overwrites the local bytes; both versions remain inspectable. |
| Content conflict | Two clients editing the same common baseline produce a conflict, not last-writer-wins. Conflict listing identifies the path and export returns both sides. Explicit resolution publishes a new baseline-bound intent. |
| Namespace conflict | Create/create, delete/edit, rename/edit, rename/rename and parent removal conflicts preserve reachable local data and expose an explicit resolution path. |
| Completion race | A v1 upload completing after local v2 appears leaves v2 pending across process restart. Completion of v1 must not acknowledge v2. |
| Unknown outcome | Service failure at the commit boundary reports uncertain completion separately from conflict and definitive failure. Restart retries the original request or resolves its receipt without replanning against a newer baseline. |
| Invalid local input | Symlinks, unsupported special files and a missing local root fail with an explicit diagnostic and do not mutate remote state. |
| Process boundary | Commands are distinct child processes. After each exits, the state lease is released and a fresh process can reopen the same state. |

`yy --volume CONFIG sync status --json` must expose accepted, pending, remotely
completed, failed, conflicted and uncertain states through a stable
machine-readable form used by the suite. The existing object-mode `yy status`
continues to report only the remote namespace. Conflict export must preserve
exact bytes. Tests must not infer completion from local fsync or from a newer
remote revision.

### Status schema v1

`yy --volume CONFIG sync status --json` always emits
`"schema": "yinyang.sync.status.v1"`. The suite decodes every fixed field,
rejects unknown enum values, requires entries in path order, and checks these
cross-field invariants:

- `summary.conflicts` equals `conflicts.length`.
- `summary.retained_artifacts` equals `artifacts.length`.
- `summary.retained_bytes` equals the sum of the retained artifact payload
  lengths reported by the replica.
- `summary.in_sync` requires an available remote and zero pending, unknown,
  conflict and failed counts.
- Every top-level conflict references an entry with the same conflict ID.

Exit code `0` means the schema was generated even if it reports pending,
unknown or conflict state. Exit code `3` reports an unbound state and `4` a
state validation error; both still emit the complete schema. The suite does not
treat either state as success.

Entries carry `path`, `original_path` and `desired_path`. `path` is the current
local path, falling back to `original_path` and then `desired_path` when local
state is absent. A conflict on an existing node uses its baseline path as both
`path` and `original_path`. A new create has no `original_path` and uses the
target for both `path` and `conflict.path`. A rename keeps the source in
`original_path` and the target in `desired_path`. A parent removal reports each
affected child by that child's baseline path.

The top-level `artifacts` array identifies every retained inode or quarantined
directory. `summary.retained_artifacts` counts that array rather than a separate
estimate. An `exportable` artifact can be read through
`yy --volume CONFIG sync export --artifact ARTIFACT_ID --output PATH`. This reads
the retained inode at export time, so bytes written through an old open
descriptor after installation must appear. Directory artifacts point at the
quarantined subtree and must fail byte-stream export instead of becoming a fake
file.

Conflict bytes are exported separately:

```shell
yy --volume CONFIG sync export --conflict CONFLICT_ID --side local --output PATH
yy --volume CONFIG sync export --conflict CONFLICT_ID --side remote --output PATH
```

The local side reads the retained local artifact when one exists. The remote
side reads the immutable node and revision fixed in the conflict record.

An explicit status or synchronization compares a retained inode with the
fingerprint captured when it was displaced. Bytes written through an old open
descriptor after installation therefore surface as a local retained-artifact
conflict on the next explicit operation. The suite exports those bytes, resolves
the conflict explicitly, and verifies the resulting remote revision. It does
not require a watcher or claim uninterrupted `in_sync` while the only local
change exists in an artifact.

## Fault boundaries

Completion-race and unknown-outcome coverage pauses or drops the real service
response at the publication commit boundary. The fault must occur after the
adapter has durably recorded the original local intent. A test that stops
before preparation, or that fails on a missing credential before reaching
publication, is a fixture failure rather than acceptance.

The test may use a loopback TCP proxy around the real metadata service. It must
not replace the service authority with an in-memory authority for the exercised
scenario, and it must independently read the service's committed receipt or
remote state after recovery.

## Entry point

The mandatory CI job invokes:

```shell
cargo x test-managed-sync
```

The task selects only `linux_sync` tests whose names begin with `managed_sync_`
and fails if the isolated S3 settings are absent. The ordinary workspace test
command still covers platform-neutral unit and integration tests; it does not
stand in for this real-process suite.

Failure logs must retain the `yy` command, exit status, stdout, stderr, test
phase, and the independent remote observation that failed. Temporary local
directories, Sync state and service databases remain inside the test temporary
directory and must not be written to the user's home or a production bucket.
