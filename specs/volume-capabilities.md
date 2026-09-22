# Volume admission and capabilities

**Status: experimental Managed volume, Mount durability policy, library frontend.**
This is a headless runtime/CLI combination, not an OS mount. Direct, Sync
reconciliation, FUSE, macOS, and Windows frontends are recognized configuration
choices but are rejected as unimplemented before startup.

## Configuration

`VolumeConfig` is strict JSON. Unknown fields and unknown enum/capability names
are rejected. A CLI configuration file contains:

```json
{
  "name": "workspace",
  "model": "managed",
  "access": "mount",
  "frontend": "library",
  "publication": "object",
  "storage_profile": "minio",
  "staging": "./workspace-staging",
  "read_only": false,
  "require": ["read", "remote-fsync", "atomic-rename"]
}
```

The CLI resolves relative staging paths against the configuration file's parent.
The storage profile is `amazon-s3` or `minio`; this is an explicit semantic
deployment selection, not detection that any S3-compatible endpoint is safe.
Storage connection/credentials remain in the existing `YINYANG_S3_*` settings.

For service publication select `"publication": "service"` and
`"service_address": "127.0.0.1:7447"`, and supply
`YINYANG_SERVICE_TOKEN` separately. The service must already be running and
bound to the same content prefix. Object publication must omit the address.
The config contains no service secret or storage credentials.

`access=mount` selects online remote-fsync semantics. `frontend=library`
executes these semantics through Runtime/FileHandle and the CLI without
installing a mount point. It does not claim an OS frontend or offline Sync.

## Negotiation and enforcement

The effective capability set is the intersection of:

1. the implemented volume model;
2. the storage prerequisites for that model and publication authority;
3. the access model and read-only policy;
4. the implemented frontend.

The storage layer reports whether its primitives can support the Managed
contract. It does not require raw S3 to implement filesystem rename. Read,
immutable streaming writes, and conditional creation are required for content;
object publication additionally requires conditional replacement. Service
publication relies on the service transaction authority for metadata atomicity.

Writable baseline capabilities are read, list, write, append, truncate,
atomic-rename, stable-identity, pinned-reads, retained-unlink-read, remote-fsync,
and recoverable-staging. The read-only baseline removes write, append,
truncate, atomic-rename, and remote-fsync. The remaining operations preserve
their definitions in the [file runtime](file-runtime.md); retained-unlink-read
does not imply writable anonymous inodes or full POSIX behavior.

Startup validates the baseline and every explicit `require` before connecting
the authority or creating/acquiring staging state. It then opens the selected
authority and initializes the runtime. Missing capabilities and unimplemented
combinations fail rather than silently downgrading.

Read-only is enforced by runtime operations, including writable open, namespace
mutations, and writes/publication through recovered handles. It is an operation
policy, not an authorization boundary against a caller that independently holds
storage credentials or invokes the low-level Authority directly. Inspecting or
reading local staged data remains possible.

## CLI

Initialize an object authority with the existing `yy create`, or a service
authority with `yy serve --database metadata.db`. Then:

```shell
yy --volume volume.json capabilities
yy --volume volume.json file create hello
yy --volume volume.json file open hello --write
# Use the returned handle UUID:
yy --volume volume.json file write HANDLE ./local-file
yy --volume volume.json status
yy --volume volume.json file fsync HANDLE
yy --volume volume.json file close HANDLE
```

`file read HANDLE` emits bytes to stdout; `--offset` and `--length` select
a range. Reads stream in bounded chunks. `file write` streams its source in
64 KiB writes, so a later local-source I/O failure can leave an acknowledged
prefix staged; it is not an atomic import of the entire source. Use
`--append` instead of `--offset` to append. `file truncate HANDLE LENGTH`
shrinks or extends. `file mkdir`, `rename`, and `unlink` publish namespace
operations. Existing handle identity is independent of path changes.

`file abort` refuses unresolved frozen publication. `file acknowledge-error`
acknowledges one handle's error; `file acknowledge-errors THROUGH` acknowledges
the durable volume ledger through its sequence. Neither changes pending data
or resolves a conflict. Read/export a conflicted handle's bytes before deciding
to abort it.

`yy --volume volume.json receipt COMMIT_ID` queries the chosen authority.
An absent receipt never proves a delayed request cannot complete. One-shot
`publish/restore` retain their original object-mode CLI and do not accept
`--volume`; they are separate from the new handle lifecycle.

## Status and limits

`status` reports local and remote handle generations, remote receipt revision,
pending/frozen/conflict state, and retained errors. It reads persisted local
state without acquiring the runtime lease. It then reports latest remote
revision or explicit unavailability. Local status remains inspectable when
the service or credentials are unavailable. The status command creates no
staging state and performs no publication.

A live in-process status additionally includes non-persisted errors when the
local disk could not save them. Another process can report only durable errors.
The latest remote revision is not evidence that every local handle is published;
compare each handle's positions and pending state.

Capability and read-only tests exercise the public startup and runtime paths,
including rejection before storage reads/local state creation. Real S3/MinIO
CLI tests exercise separate processes for open/write/fsync/reopen, both
publication authorities, and local status with the service stopped.
