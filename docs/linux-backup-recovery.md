# Linux backup and isolated recovery

**Status: experimental, quiesced backup and bounded local-profile restore.**

This procedure preserves one consistent point in time across the YinYang
object metadata, immutable content, metadata-service database, local Mount/Sync
state, retained artifacts, and publication journals. It is not an online
snapshot protocol: every writer must be stopped before the backup begins.

Restore a `YYBK0001` package with supported local and SQLite profiles. The
bounded `fd1338c2` local-state transition may be upgraded by the normal writer
after isolated copy. General cross-version migration and downgrade are not
provided.

## Freeze writers

Before copying anything:

1. Stop every `yy serve` process that owns the metadata database.
2. Unmount or stop every `yy mount` process. Do not copy while a mount holds
   its staging lease.
3. Let every finite `yy --volume CONFIG sync run`, `resolve`, `publish`, and
   `publication retry` process exit. Do not start another writer until the
   backup is complete.
4. Stop other clients that can write the object prefix or local root. A process
   that may still complete a request cannot be represented as a safe backup.
5. Keep every backup outside the synchronized root and state directories.

An absent receipt is not evidence that a request failed. If an uncertain
operation may still be running, stop it and include its complete durable state
in the backup; do not delete the state or manufacture a missing receipt.

## What to preserve

| Component | State that must be copied |
| --- | --- |
| Object authority | Every object below the configured operator root, including `.yinyang/head` and all immutable content/index objects. |
| Metadata service | A consistent SQLite backup of the service database. Its WAL contains committed state. |
| Mount/file runtime | A consistent SQLite backup of `staging.db`. Chunks, frozen plans, local generations, and the error ledger are inside it. |
| Managed Sync | `replica.db`, `sync.db`, `content/staging.db`, retained `quarantine/` bytes, and the bound local root. |
| One-shot publication | `directory.db` and the immutable object content referenced by its frozen requests. The original source is not needed for retry or export. |

Do not copy only a live SQLite main file. Either freeze the owning process and
use the SQLite backup API (for example `sqlite3 DB ".backup DEST"`), or use a
filesystem snapshot that captures the database and WAL together.

## Object authority

After writers are frozen, copy the complete operator prefix to an isolated
prefix. A recursive S3 copy is sufficient:

```shell
aws s3 cp "s3://BUCKET/OLD_ROOT/" "s3://BUCKET/NEW_ROOT/" --recursive
```

Provider-native versioning or inventory export may replace the example copy,
but the result must contain all objects below the old root. Do not copy only
`.yinyang/head`: the head references immutable metadata and content objects.

Point the restored environment at the new prefix with a new
`YINYANG_S3_ROOT`, then validate it with `yy status`, `yy receipt UUID`, or
`yy restore DEST --revision REV`.

## Metadata service

With the service stopped, or through a supported SQLite online backup while all
writers are frozen:

```shell
sqlite3 /srv/yinyang/metadata.db ".backup '/backup/metadata.db'"
sqlite3 /backup/metadata.db "PRAGMA integrity_check;"
```

Copy the matching immutable object prefix as described above. To restore into
an isolated environment, place the backup database at a new path, start
`yy serve --database /new/path/metadata.db` with the new object root, and keep
the clients on matching versions. Existing receipts, historical revisions, and
commit outcomes are retained in that database.

## Mount and file staging

Stop the mount or file writer, then copy its `staging.db` with a consistent
SQLite backup:

```shell
sqlite3 /var/lib/yinyang/mount/staging.db \
  ".backup '/backup/mount/staging.db'"
```

Restore the database into an empty staging directory at the configured path.
`runtime.lock` is an ephemeral process lease and is not backup data. Partial
opens are discarded on the next open; accepted writes, frozen requests, and
the error ledger remain recoverable. Use `yy --volume CONFIG status` to inspect
the restored state and `yy --volume CONFIG file fsync HANDLE` to retry a frozen
request when the authority is available.

## Managed Sync package

Managed Sync needs more than a SQLite copy because its baseline, work records,
install records, retained artifacts, and local root contain cross-referenced
filesystem identities. Use the dedicated package command:

```shell
yy --volume CONFIG sync backup --output /backup/notes-sync
```

The command takes the existing writer leases, validates the live Sync and
staging profiles, and creates a new directory containing:

```text
manifest.bin
root/
state/replica.db
state/sync.db
state/content/staging.db
state/quarantine/
```

The SQLite files are created with `VACUUM INTO`, not by copying a live main
database. `manifest.bin` records the filesystem identity, the complete
quiesced local snapshot with content hashes, each retained artifact's current
tree and hash, the SQLite file hashes, and the original filesystem identities.
It is an experimental package format, not a public migration or online
replication format.

The backup command refuses a destination inside the synchronized root or
state, refuses a non-terminal local install, and verifies the copied files
before reporting success. A failed backup directory is left in place for
inspection; it must not be used as a successful package.

Restore into new, absent paths:

```shell
yy --volume CONFIG sync restore \
  --backup /backup/notes-sync \
  --directory /new/notes
```

The destination state is the `staging` path in `CONFIG`; both that directory
and `--directory` must be absent. Before creating either destination, the
command validates the manifest checksum, profile, filesystem, SQLite hashes,
staging handles, artifact names, and path overlap. A wrong authority, malformed
manifest, unsafe relative path, or duplicate identity fails without creating
the restore destination.

After copying, restore remaps the original root and quarantine inode identities
to the new filesystem identities. It preserves accepted content fingerprints,
commit IDs, Unknown requests, and historical artifact digests verbatim. This
mapping is what preserves an unscanned local rename through `detect_moves` and
what prevents a changed retained artifact from being mistaken for a different
file. The mapping is written in one SQLite transaction; invalid or ambiguous
old-to-new identity collisions fail before `replica.db` is rebound.

A copied root can contain local edits that were not yet reconciled. They remain
local edits and are reported by `sync status`; restore does not invent an
accepted baseline entry for them. A retained artifact is copied at its current
bytes, even if those bytes differ from the install's historical expected
digest. The existing Sync rules then report the retained conflict rather than
silently accepting or deleting the bytes.

## One-shot publication journal

Stop the publishing process, then back up its journal:

```shell
sqlite3 /var/lib/yinyang/publication/directory.db \
  ".backup '/backup/publication/directory.db'"
```

Copy the immutable object prefix that the journal references. The source
directory is not needed after preparation. Restore the database at a new
`--state` path and inspect it without storage credentials:

```shell
yy publication inspect COMMIT_ID --state /restored/publication --json
yy publication export COMMIT_ID --state /restored/publication --destination DIR
yy publication retry COMMIT_ID --state /restored/publication
```

`export` and `retry` use the restored object prefix through
`YINYANG_S3_ROOT`. A restored `Unknown` intent remains Unknown until its
original CommitId is found in a receipt or the exact frozen request is retried.
An absent receipt alone does not prove that a delayed commit did not happen.

## Failure and recovery checks

- Backend outage: stop the authority or object endpoint. A client write,
  fsync, or Sync run must report the real failure. Locally accepted bytes and
  pending/Unknown state must remain in the restored state.
- Credential failure: run the same command with invalid credentials. The
  command must fail before publication; it must not discard local state.
- Local state exhaustion: use a bounded, isolated tmpfs or an equivalent
  scoped fixture, acknowledge a small write, then append past capacity. The
  failed append must leave the acknowledged prefix readable after a new process
  opens the state. Restore capacity on that fixture, explicitly fsync the same
  handle, and verify the resulting remote bytes independently.
- Process restart: reopen the state in a new `yy` process. Inspect with
  `yy --volume CONFIG status`, `yy publication inspect`, or `yy sync status`
  before retrying anything.
- Uncertain commit: preserve the original CommitId. Retry only the exact
  frozen request; do not replan from a changed source or infer failure from an
  absent receipt.

After restore, verify at least the retained receipt/revision, one historical
restore, one `Unknown` request's exact CommitId, any pending local bytes, and
the exported bytes of every retained artifact. The current tests exercise
these paths with real `yy` processes and an isolated S3-compatible authority.

## Limits

This is a quiesced procedure with only the local-state compatibility explicitly
documented for `fd1338c2`. It does not provide online consistency, distributed
leases, backup encryption, deduplication, garbage collection, general
cross-version migration, or a promise that a copied database is usable without
its immutable content prefix. A filesystem copy must preserve the supported
filesystem semantics; use isolated restoration rather than overwriting a live
root.
