# Volume admission and capabilities

**Status: experimental Managed volume, Mount durability policy, library and
Linux FUSE frontends, conditional-write S3 Direct, and Linux ordinary-directory
Managed Sync.**
Writable Direct is opt-in and requires local staging. Direct Sync is a separate
library workflow for the combinations admitted below. macOS and Windows
frontends are recognized configuration choices but rejected as unimplemented
before startup; read-only Managed Sync is rejected as unsupported. The Linux
FUSE frontend is specified separately in [`linux-fuse.md`](linux-fuse.md), Direct in
[`direct.md`](direct.md), and Managed Sync in [`linux-sync.md`](linux-sync.md).

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

A Direct configuration uses the same strict document without `staging`:

```json
{
  "name": "objects",
  "model": "direct",
  "access": "mount",
  "frontend": "fuse",
  "publication": "object",
  "storage_profile": "amazon-s3",
  "require": ["read", "list", "pinned-reads"]
}
```

Direct `read_only` defaults to `true`; Managed retains its explicit or default
`false` write policy and required staging path. A read-only Direct mount
permits only `access=mount`, object publication, `frontend=library`, or Linux
`frontend=fuse`, and must omit staging. A writable Direct mount sets
`read_only=false`, provides a staging path, and uses Linux `frontend=fuse`.
Direct does not accept a service address or use a publication service.

Direct Sync is `access=sync`, Linux `frontend=library`, object publication,
`read_only=false` and a non-empty staging path. It requires the same conditional
create/replace primitives as writable Direct plus conditional delete. The
staging path is the Direct Sync state directory; the ordinary `file` runtime
and `Volume::open` do not install Direct Sync handles.

For service publication select `"publication": "service"` and
`"service_address": "127.0.0.1:7447"`, and supply
`YINYANG_SERVICE_TOKEN` separately. The service must already be running and
bound to the same content prefix. Object publication must omit the address.
The config contains no service secret or storage credentials.

`access=mount` selects online remote-fsync semantics. `frontend=library`
executes these semantics through Runtime/FileHandle and the CLI without
installing a mount point. It does not claim an OS frontend or offline Sync.
`frontend=fuse` requires Linux. A read-only configuration mounts with kernel
`ro` and refuses unpublished staging before serving. A writable configuration
opens the shared Mount read-write; recovered pending/frozen/error state is
served through the same handles and reported by flush/fsync rather than hidden.

Managed `access=sync` selects bidirectional ordinary-directory reconciliation
and is admitted only with `frontend=library` and `read_only: false`. The
[`sync` CLI](linux-sync.md) connects the authority through `Volume::connect`;
the file-runtime opener `Volume::open` still rejects Sync volumes because it
installs recoverable handles instead of reconciling a directory.

## Negotiation and enforcement

The effective capability set is the intersection of:

1. the implemented volume model;
2. the storage prerequisites for that model and publication authority;
3. the access model and read-only policy;
4. the implemented frontend.

Startup also requires the intersection of the volume, access, and frontend
contracts (the combination baseline) to survive the storage intersection, so a
frontend cannot silently weaken the model. The library frontend claims the full
implemented Managed runtime set. A read-only Managed Linux FUSE mount claims
read, list, stable-identity, pinned-reads, and retained-unlink-read; a writable
FUSE mount claims the full Managed set, including write, append, truncate,
atomic rename, remote fsync, and recoverable staging.
A read-only Managed Sync configuration is rejected before startup because
reconciliation is bidirectional and the flag would otherwise be silently
ignored while the volume still published writes. Read-only Direct claims read,
list, and pinned-reads. Writable Direct adds write, append, truncate,
remote-fsync and recoverable-staging. Direct never claims Managed
stable-identity, atomic-rename, retained-unlink-read, or Managed receipts.
macOS/Windows frontends claim an empty set and are rejected by their own
admission rule.

The storage layer reports whether its primitives can support the Managed
contract. It does not require raw S3 to implement filesystem rename. Read,
immutable streaming writes, and conditional creation are required for content;
object publication additionally requires conditional replacement. Service
publication relies on the service transaction authority for metadata atomicity.
Read-only Direct requires stat, read, recursive list, version reads, and
conditional ETag reads. Writable Direct additionally requires write, whole-file
multi-write support, empty writes, conditional create, and conditional ETag
replace. Direct Sync additionally requires conditional ETag delete. Direct never
negotiates Managed transaction primitives. The selected service must also enumerate current keys exactly:
capability flags cannot repair a lossy LIST that hides `a/b` when file key `a`
exists, because the mount layer cannot read a key name it never receives.
The current Silo/MinIO qualification does not cover that file-prefix-alias
namespace combination; observed collisions still reject, and ordinary
namespaces that LIST completely remain supported.

Managed writable baseline capabilities are read, list, write, append, truncate,
atomic-rename, stable-identity, pinned-reads, retained-unlink-read, remote-fsync,
and recoverable-staging. The read-only Managed baseline removes write, append,
truncate, atomic-rename, and remote-fsync. The remaining operations preserve
their definitions in the [file runtime](file-runtime.md); retained-unlink-read
does not imply writable anonymous inodes or full POSIX behavior. Direct has the
separate capability projection above and never inherits the Managed baseline.

Startup validates the baseline and every explicit `require` before connecting
the authority or creating/acquiring staging state. It then opens the selected
authority and initializes the runtime. Missing capabilities and unimplemented
combinations fail rather than silently downgrading.

The mount entry point independently requires `frontend=fuse` and repeats full
admission before observation. A valid library configuration therefore cannot
be mounted by mistake, and a Direct mount cannot bypass writable, Sync, service
or required-capability rejection.

Read-only is enforced by runtime operations, including writable open, namespace
mutations, and writes/publication through recovered handles. It is an operation
policy, not an authorization boundary against a caller that independently holds
storage credentials or invokes the low-level Authority directly. Inspecting or
reading local staged data remains possible.

## CLI

Initialize an object authority with the existing `yy create`, or a service
authority with `yy serve --database metadata.db`. Then:

```shell
yy --volume volume.json mount /mnt/workspace
yy --volume volume.json capabilities
yy --volume volume.json doctor --json
yy --volume volume.json file create hello
yy --volume volume.json file open hello --write
# Use the returned handle UUID:
yy --volume volume.json file write HANDLE ./local-file
yy --volume volume.json status
yy --volume volume.json file fsync HANDLE
yy --volume volume.json file close HANDLE
```

Direct uses `mount`, `capabilities`, `doctor`, and `status`; `file`, Managed
`receipt`, and `sync` commands reject it. Direct `status` reports the accepted
recursive observation summary without creating local staging. A Direct
`capabilities` or `doctor` call never creates storage metadata.

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

`doctor` is the non-destructive operator-readiness projection. It reads the
strict volume document, reports whether the current host is the tested
operational combination, checks FUSE device/helper access, and validates
admission. Managed performs one owner-only temporary state-path write probe,
removes it, inspects persisted Mount records read-only, and observes the selected
authority. Direct performs no state probe and reports a recursive namespace
observation instead. Doctor does not create staging, initialize `staging.db` or
`sync.db`, run recovery, acquire a runtime or publication lease, or publish. Its
JSON form and exit codes are specified in
[`linux-operations.md`](linux-operations.md).

`doctor` reports readiness, not reconciliation convergence. In particular, a
successful finite `sync run` and a successful `doctor` check do not establish
that local and remote state match; only `sync status --json` with
`summary.in_sync=true` and an available remote does.

## Status and limits

`status` reports local and remote handle generations, remote receipt revision,
pending/frozen/conflict state, and retained errors. It reads persisted local
state without acquiring the runtime lease. It then reports latest remote
revision or explicit unavailability. Local status remains inspectable when
the service or credentials are unavailable. The status command creates no
staging state and performs no publication.

For Direct, `status` reports the namespace observation digest and root entry
counts. It has no local handles, remote receipt revision or staging lease.

A live in-process status additionally includes non-persisted errors when the
local disk could not save them. Another process can report only durable errors.
The latest remote revision is not evidence that every local handle is published;
compare each handle's positions and pending state.

Capability and read-only tests exercise the public startup and runtime paths,
including rejection before storage reads/local state creation. Real S3/MinIO
CLI tests exercise separate processes for open/write/fsync/reopen, both
publication authorities, and local status with the service stopped.
