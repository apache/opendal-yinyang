# Linux operational diagnostics and lifecycle

**Status: experimental operator workflow for Ubuntu 24.04 x86_64.**
This specification defines the `yy doctor` health boundary and the supported
user-service lifecycle around existing Linux frontends. It does not add a new
daemon, automatic Sync watcher, HA authority or packaging system.

## Qualification boundary

The installed workflow is qualified as a named host, frontend, publication
authority, storage backend and exact binary identity. Ubuntu 24.04 x86_64 is
the initially tested host combination. It is not a runtime-only restriction:
other Linux hosts may run the CLI, but they are not presented as qualified.

Object-backed Managed Mount and finite Managed Sync are one matrix dimension.
Authenticated loopback metadata service publication is a separate dimension.
Amazon S3 and MinIO-compatible storage remain separate backend entries even
when both use the same S3 provider implementation. A MinIO result does not
establish a non-loopback S3 deployment claim.

Every lifecycle run records the source commit, binary BLAKE3 or SHA-256,
storage backend, endpoint class, isolated prefix, working directory, commands,
exit status and retained state paths. The service token and storage
credentials are never part of source, command arguments, state metadata or
test output.

## `yy doctor`

```text
yy --volume CONFIG doctor [--json]
```

`doctor` performs a bounded readiness check without initializing the volume or
acquiring its writer lease. It requires a readable strict volume document and
reports `yinyang.operational-health.v1` with this shape:

```text
{
  "schema": "yinyang.operational-health.v1",
  "status": "pass|action_required",
  "tested_host": true,
  "config": "...",
  "volume": {
    "name": "...",
    "model": "managed|direct",
    "access": "mount|sync",
    "frontend": "fuse|library",
    "authority": "object|authenticated-loopback-service"
  },
  "scope": {
    "state_probe": "...",
    "catalog_and_publication_state": "...",
    "authority": "..."
  },
  "checks": [
    { "name": "platform", "status": "pass|action_required|info",
      "detail": "...", "remediation": null }
  ]
}
```

The command always emits a complete report for a readable configuration, even
when a check fails. Exit code `0` means every check passed and every check not
applicable to the selected combination was omitted. Exit code `1` means at
least one check requires action or the report could not be written. Invalid
CLI arguments remain Clap exit code `2`.

Checks are ordered from local policy to remote observation:

- `platform`: `tested_host` is true only on Ubuntu 24.04 x86_64. Another Linux
  host receives an informational check and does not become unhealthy merely
  because it is untested. A non-Linux host reports the operational restriction
  instead of implying native Linux frontend support.
- `configuration`: parses the strict volume document and resolves a non-empty
  relative staging path against the document directory. Read-only Direct has no
  staging path; writable Direct requires one.
- `environment`: requires a bucket, a complete explicit access-key pair when
  either key is set, and, for service publication, a 32..=1024 byte service
  token. The volume document's `storage_profile` is authoritative for a
  `--volume` command; `YINYANG_STORAGE_PROFILE` is not required or compared.
  Ambient AWS credential chains remain valid when no explicit pair is present.
- `fuse-device` and `fuse-helper`: for `frontend=fuse`, open `/dev/fuse`
  read/write and close it without mounting, then require `fusermount3` on
  `PATH` to pass the current user's execute-access check. File existence alone
  is not treated as readiness.
- `local-state`: Managed writes one namespaced owner-only temporary file under
  the nearest existing state-directory ancestor, syncs and removes it, or probes
  the existing state directory. Writable Direct performs the same owner-only
  temporary-file probe and then inspects retained Direct runtime state
  read-only; read-only Direct reports this check as informational. Doctor does
  not create the configured staging directory, initialize `staging.db`,
  `sync.db` or Direct `runtime.db`, run recovery, acquire the runtime or
  publication lease, or inspect a live Mount as unhealthy merely because that
  Mount holds its lease.
- `mount-state`: when existing Mount staging is present, inspect persisted
  handles without taking the lease. For writable Direct staging, inspect the
  retained runtime profile, schema, binding and pending/conflict/Unknown state
  without creating or recovering it. Pending, frozen, conflict, Unknown and
  failed records require action; read-only inspection itself is not a failure.
- `admission`: construct the S3 operator and validate the effective
  four-layer capability intersection.
- `authority`: Managed connects and observes the latest revision. Object mode checks the
  storage authority. Service mode uses the configured loopback address and
  separately supplied token; the check fails if either authentication or
  reachability fails. No mutation is issued.
- `namespace`: Direct validates the storage-native mapping and performs one
  recursive read-only object observation. Invalid names, file-prefix
  collisions, unsupported storage, authentication failure and unavailable
  endpoints require action. No object or YinYang metadata is created.
- `sync-convergence`: for `access=sync`, this is informational only. A finite
  `sync run` exit code is not convergence. The operator must run `sync status
  --json` and require `summary.in_sync=true` with an available remote.

Every detail and remediation string is redacted before human or JSON output.
The redactor removes configured authentication values as well as URL userinfo.
Diagnostics may name environment variables and
classify a 403, connection failure or state-write failure, but they do not
prove which credential or policy caused a remote rejection.

## User-service lifecycle

The repository supplies optional, non-enabled systemd user unit examples under
`deploy/linux/`:

- `yinyang-serve.service` runs the loopback metadata authority;
- `yinyang-mount@.service` runs one foreground Managed Mount;
- `yinyang-sync@.service` runs one finite Managed Sync pass and retains the
  successful `active (exited)` result until stopped or restarted.

All units use a mode `0600` common environment file supplied separately from
the unit files. Instance units load an optional owner-only
`environment-<instance>` overlay after the common file so each Mount or Sync
instance can select its own storage root. The service unit uses the
`environment-service` overlay. Units run in the installing user's manager and
do not install host-level services, enable lingering, start at boot by default
or alter system-wide FUSE configuration.

The Mount unit must not set `NoNewPrivileges=yes`: an unprivileged user mount
may rely on the setuid `fusermount3` helper. It uses `UMask=0022` because the
current FUSE frontend represents only `0644` and `0755` file modes; a stricter
mask would make ordinary creates appear as unrepresentable `0600` files.
Configuration, state and environment directories remain private (`0700`) and
the common environment file remains `0600`.

Mount and service units are restartable long-running processes. The Sync unit
is oneshot: active/exited means the command completed successfully, never that
the volumes converged. Its restart performs another finite pass.

Start and stop preserve all configured staging directories, SQLite databases,
receipts, WAL files, publication journals and local synchronized data. A
successful stop is not a backup. Removing the versioned binary prefix and unit
files does not imply deleting state or configuration.

## Installed binary transitions

The supported experimental upgrade route is a stopped, same-host replacement
of one immutable binary release:

1. Stop every Mount, finite Sync and metadata-service unit.
2. Preserve the complete configuration, Mount staging, Sync state and metadata
   database including SQLite WAL files.
3. Install the new binary into a new release directory, verify its identity,
   and atomically replace the `current` symlink while no `yy` writer runs.
4. Reopen the retained state only with a binary that advertises every persistent
   profile reached by that state.
5. Require the source data, pending and frozen handles, original CommitIds,
   receipts, retained local artifacts, SQLite integrity and finite Sync status
   to survive before accepting the transition.

An exact producer and target source pair is a qualification result recorded in
the operations guide. The general contract is not an arbitrary upgrade matrix.
The first producer is `fd1338c2f924a90e9da752b9158e748d1d3acaf4`; a later
accepted target is recorded only after the installed transition passes against
the final target binary.

Downgrade after the newer binary has opened or written local or authoritative
state is unsupported. Keeping an older immutable release directory for audit or
reinstallation does not grant downgrade permission. Hot replacement of a
running binary, migration performed only because a release directory exists,
and compatibility inferred from an equal package version are also unsupported.
The package version and release directory identity are separate.

## Verification

`tests/linux_operations.rs` exercises:

- `doctor` admission, action reporting and secret redaction through the real
  binary;
- a clean copied installation prefix with a separate working directory;
- object- and service-backed Managed Mount start, mounted write, fsync, read,
  restart, stop and retained staging;
- object- and service-backed finite Sync whose result is accepted only through
  `summary.in_sync=true`;
- authenticated loopback service start, stop and restart against retained
  SQLite authority;
- removal of the installed binary prefix while state and configuration remain;
- when run with isolated MinIO and a systemd user manager, actual start,
  restart and stop of the three user units through `systemctl --user`.

`tests/linux_prefix_upgrade.rs` separately exercises a stopped installed-prefix
transition from the exact `fd1338c` producer to the selected target binary,
including retained pending, frozen and Unknown work, receipts and a non-empty
service WAL. It uses the same versioned `current` prefix as the user units while
keeping process lifetime independent of a particular systemd user session.

The real Amazon S3 smoke inherits `YINYANG_S3_SESSION_TOKEN` when temporary
credentials are used. It must not inherit a MinIO endpoint and does not use a
counting proxy. A real Amazon S3 result is recorded only when the actual CLI
completes the isolated smoke against the named bucket and region.

Current evidence covers the installed exact CLI and systemd user-unit
lifecycles for object and authenticated-loopback-service Mount and Sync,
including mounted write, fsync, read, restart and convergence checks. Exact
source, binary, backend and region identities are recorded in the
[Linux operations guide](../docs/linux-operations.md#qualification-matrix).
This scoped backend and host evidence is not a capacity, multi-host or HA
claim.
