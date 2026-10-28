# Linux ordinary-directory Managed Sync

**Status: experimental.**

The `yy --volume CONFIG sync` frontend reconciles one ordinary Linux directory
with a Managed volume's remote root. It is the failure and progress surface for
the reconciliation contract: it fixes admission, exit codes and a versioned
status document so a script or a test can decide what happened without parsing
logs. The reconciliation itself lives in `yinyang::replica::DirectorySync`; the
frontend only binds a volume configuration to that responsibility and projects
its evidence.

## Commands

```text
yy --volume CONFIG sync init    --directory ROOT
yy --volume CONFIG sync backup  --output BACKUP
yy --volume CONFIG sync restore --backup BACKUP --directory ROOT
yy --volume CONFIG sync run
yy --volume CONFIG sync status  [--json]
yy --volume CONFIG sync export  --artifact ARTIFACT_ID --output PATH
yy --volume CONFIG sync export  --conflict CONFLICT_ID --side local|remote --output PATH
yy --volume CONFIG sync resolve CONFLICT --keep local|remote
yy --volume CONFIG sync resolve CONFLICT --content PATH
```

- `init` binds `ROOT` to the volume's remote root. It creates the local root and
  the state directory when missing, rejects a state directory that contains the
  root (or the reverse), and requires root and state on the same filesystem.
- `backup` writes one experimental quiesced package containing the bound root,
  `replica.db`, `sync.db`, content staging, retained artifacts, and a manifest
  binding their filesystem identities and current content hashes. It requires
  exclusive Sync/runtime leases and refuses incomplete local installs.
- `restore` validates a backup package and the configured authority before
  creating absent destination root/state paths. It copies the package, then
  rebinds root/quarantine inode identities in one SQLite transaction. It
  preserves accepted fingerprints, commit identities, Unknown work, and
  historical artifact digests.
- `run` performs one finite reconciliation pass: it retries any associated
  durable work, pulls remote changes, and publishes eligible local edits.
- `status` reports the current binding, remote and local observations, and the
  derived per-path state. It never mutates state or acquires the publication
  lease, so it stays usable while `run` holds the lease.
- `export` writes retained bytes. `--artifact` selects a retained artifact from
  `status`; `--conflict` with `--side` selects one side of a conflict. The output
  path must not exist.
- `resolve` applies one durable decision to an active conflict: `--keep` selects
  the local or remote observation, `--content` adopts explicit bytes from a file.
  A decision that publishes local bytes goes through the same work/receipt path
  as an ordinary edit; keeping the remote observation applies the recorded remote
  state locally without a new publish, and an already-matching choice can be a
  no-op.

## Admission

`sync` requires a Linux host and a volume that is exactly:

```json
{
  "model": "managed",
  "access": "sync",
  "frontend": "library",
  "read_only": false
}
```

It runs the shared four-layer capability admission as every other volume
command and additionally pins this Managed + Sync combination, so a valid Mount
package or a Direct volume is rejected rather than silently reinterpreted. The
frontend is the library frontend, not an OS mount. The frontend performs
bidirectional reconciliation, so `read_only: true` is rejected for `access:
sync` instead of being silently ignored while the volume still published writes.
Storage credentials and the `--volume` configuration are supplied exactly as for
the other volume commands; the JSON document never carries secrets.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success, or a valid bound `status` document (including pending, conflict or unknown entries). |
| 3 | `status` found no binding. The command still prints a valid document. |
| 4 | `status` found an invalid binding, such as a missing bound root. The command still prints a valid document. |
| 1 | Argument, configuration, admission or other unexpected failure. |

`run` and `resolve` succeed whenever a finite pass completed, even if work is
still pending (for example, an observed remote completion with a newer local
edit). They fail only on an unresolved `Unknown` outcome: a request that may
still have published and whose receipt is missing. `status` must be consulted to
decide whether to retry.

## Offline behavior

`status` and `export --artifact` work without remote connectivity. `status`
classifies the local binding first and reports remote availability truthfully,
marking it unavailable with the real connection error when the authority cannot
be reached; it never fabricates success. `export --artifact` and `export
--conflict --side local` read durable local bytes and never contact the
authority. `export --conflict --side remote` reads a retained revision and
needs the authority.

## Backup identity

The root and state directory identities recorded in `replica.db` are not
portable across an ordinary filesystem copy. The backup manifest therefore
contains complete quiesced `LocalSnapshot` evidence for the root and each
retained artifact, including original inode relationships and current content
hashes, plus hashes of the consistent SQLite package files. Restore verifies
the package, then maps original root/quarantine identities to the copied
destinations. Baseline and work fingerprints and CommitIds are not rewritten.

An unscanned local rename remains a rename because the old inode is mapped to
the copied inode at its new path. Local edits not represented by baseline/work
remain local differences. A retained artifact is validated against the bytes
captured by the quiesced backup, not against the install's historical
`expected_digest`; the existing conflict rules may therefore report the
retained artifact as conflicted without discarding its bytes.

Restore requires the supported manifest profile, documented local component
profiles, a matching filesystem, absent destination paths, and terminal install
records. It rejects malformed
checksums, unsafe/duplicate paths or artifact names, wrong profiles, incomplete
installs, and ambiguous old-to-new identity collisions before creating or
rebinding destinations. The package format is experimental and is not an
online snapshot or migration contract. See
[Linux backup and isolated recovery](../docs/linux-backup-recovery.md).

Before package copy, restore validates `replica.db`, `sync.db`, and
`content/staging.db` read-only and binds the manifest to the replica binding.
The package container remains `YYBK0001`. Supported `fd1338c2` local profiles
may be restored and are upgraded only by the normal writer open after the
isolated destinations exist; unknown profiles or partial known schemas fail
without creating either destination. Other cross-version restore combinations
remain unsupported.

## Status document

`status --json` prints one `yinyang.sync.status.v1` document. Every field is
always present; an unknown value is `null` rather than omitted. Enumerations are
`snake_case`. Digests are 32-byte hex, node and commit identities are 16-byte
hex, and revisions are 24-byte hex.

```text
{
  "schema": "yinyang.sync.status.v1",
  "binding": { "state": "bound|unbound|error", "state_directory": "...",
               "root": "...", "filesystem": "...", "same_filesystem": true },
  "remote":  { "availability": "not_checked|available|unavailable",
               "revision": "...", "error": null },
  "local":   { "scan": "not_scanned|clean|changed|failed", "error": null },
  "summary": { "in_sync": false, "pending": 0, "unknown": 0, "conflicts": 0,
               "failed": 0, "retained_artifacts": 0 },
  "entries": [ {
    "path": "...", "original_path": null, "desired_path": null,
    "state": "in_sync|local_pending|remote_pending|conflict|unknown|failed",
    "local":  { "kind": "file|directory", "length": 0, "digest": "...",
                "executable": false, "presence": "present|absent|unknown" },
    "remote": { "kind": "file|directory", "length": 0, "digest": "...",
                "executable": false, "presence": "present|absent|unknown",
                "node": "...", "revision": "..." },
    "baseline": { "node": "...", "revision": "..." },
    "intent": { "id": "...", "operation": "content|create_file|create_directory|remove|rename|set_executable",
                "state": "prepared|unknown|committed|conflict|retryable|failed",
                "commit_id": "...", "receipt_revision": "..." },
    "outcome": { "state": "pending|remote_completed|conflict|unknown|retryable|failed|none", "error": null },
    "conflict": null
  } ],
  "conflicts": [ { "id": "...", "path": "...", "original_path": null, "desired_path": null,
                   "local": "current_file|current_directory|retained_artifact|deleted|unknown",
                   "remote": "current_file|current_directory|retained_artifact|deleted|unknown",
                   "local_artifact": null, "remote_artifact": null,
                   "remote_revision": "...", "error": "..." } ],
  "artifacts": [ { "id": "...", "origin": "...",
                   "kind": "replaced_file|rollback_displaced|quarantined_file|quarantined_directory|upload_retained",
                   "phase": "preparing|prepared|moved|retained|conflict|failed",
                   "path": "...", "exportable": true, "error": null } ]
}
```

`entries` is sorted by `path`. `summary.conflicts` equals the number of
top-level conflicts and `summary.retained_artifacts` equals the number of
artifacts. An entry in conflict carries the same conflict identity in its nested
`conflict` object. `summary.in_sync` is true only after a valid binding and
baseline, an observed remote, no unresolved work, and an actual local/remote
match.

## Example

```shell
export YINYANG_S3_BUCKET=my-bucket
export YINYANG_S3_REGION=us-east-1
export YINYANG_S3_ROOT=my-filesystem
export YINYANG_STORAGE_PROFILE=minio
export YINYANG_S3_ENDPOINT=http://127.0.0.1:9000

cat > sync.json <<'JSON'
{
  "name": "notes",
  "model": "managed",
  "access": "sync",
  "frontend": "library",
  "publication": "object",
  "storage_profile": "minio",
  "staging": "/var/lib/yinyang/notes-state",
  "read_only": false,
  "require": []
}
JSON

yy --volume sync.json sync init --directory "$HOME/notes"
yy --volume sync.json sync run
yy --volume sync.json sync status --json
```

## Limitations

`sync` is implemented only on Linux and drives an ordinary directory; it does
not provide macOS File Provider integration, an automatic watcher or a daemon.
A retained artifact's bytes and a conflict's local side are exported offline; a
conflict's remote side needs the authority.
