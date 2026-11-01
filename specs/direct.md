# Direct volumes

**Status: experimental S3-compatible read-only library/Linux FUSE support and
conditional mutation library runtime.**

A Direct volume keeps the configured storage namespace authoritative. It lists
and reads existing objects without creating YinYang metadata, Managed NodeIds,
content proofs, transactions or Managed identities. This specification defines
the read-only observation and FUSE contract. The separate
[Direct mutation runtime](direct-mutations.md) defines conditional publication,
retained local generations and safe-save namespace mutation. Writable FUSE
admission is defined by the Linux FUSE frontend.

## Configuration and admission

A Direct volume uses this configuration shape:

```json
{
  "name": "objects",
  "model": "direct",
  "access": "mount",
  "frontend": "fuse",
  "publication": "object",
  "storage_profile": "minio",
  "read_only": true
}
```

`read_only` defaults to `true` for Direct and must be `true` when present.
`staging` must be omitted. The existing Managed default remains
`read_only=false` with a required staging path.

Direct admission accepts only:

- `access=mount`;
- `publication=object` with no service address;
- `frontend=library`, or `frontend=fuse` on Linux;
- the `amazon-s3` and `minio` profiles;
- an S3 operator that reports `stat`, `read`, recursive `list`, version reads,
  and conditional ETag reads. Both read pin modes are required because bucket
  versioning state does not prove that every object has an immutable version;
- a conforming backend whose LIST returns every current object key exactly.
  A service that hides a descendant such as `a/b` merely because file key `a`
  exists cannot be repaired by the mount layer: with no key name, no
  read-only API can discover the omitted object. Capability flags describe
  operations, not this exact-enumeration property, so selecting a conforming
  service remains an operator prerequisite.

The mount entry point also requires `frontend=fuse`. Unsupported access,
publication, frontend, write policy or required capabilities fail before the
first storage observation and before a FUSE session is created.

The current qualification does not cover a MinIO-compatible service whose LIST
omits a descendant key such as `a/b` when file key `a` exists. The isolated
Silo endpoint used by the native suite can GET both keys but has demonstrated
that omission. A namespace with a returned file-prefix collision still rejects;
the unqualified case is the backend that never returns the hidden key name to
the mount layer.

The read-only frontend publishes the `read`, `list`, and `pinned-reads`
capabilities only. It does not claim Managed `stable-identity`: replacement
changes the file inode. It does not claim `retained-unlink-read`; retained reads
after deletion depend on backend version history and are not a portable Direct
guarantee. Writable admission checks the native primitives through the shared
admission table and the mutation runtime specification.

## Reversible key mapping

An object key is split on `/` into UTF-8 path components. File keys must not
contain empty components. A key ending in `/` is a directory marker only when
its object length is zero; a non-empty marker is rejected rather than hidden.
The storage listing may identify a trailing-slash key as a directory without
exposing its object length, so Direct confirms every listed marker with a
one-byte range GET pinned by the listed ETag. Any returned byte rejects the
complete observation.

Each component is mapped as follows:

- a literal `%` becomes `%25`;
- NUL, ASCII controls and DEL become uppercase `%HH`;
- the exact components `.` and `..` have every byte percent-encoded;
- all other UTF-8 bytes are preserved as-is.

Because `%` is always escaped, an encoded token cannot collide with a literal
key component. Case and Unicode normalization are not changed or folded:
`A`, `a`, composed and decomposed forms remain distinct Linux names. An
encoded component above 255 bytes, or an encoded path above 4095 bytes, is
rejected.

The observation also rejects, with the original object keys in the diagnostic:

- an empty, leading or repeated path component;
- a non-empty directory marker;
- a file-prefix collision such as keys `a` and `a/b`;
- any two keys that map to one path.

One unrepresentable object returned by a conforming LIST rejects the complete
observation. The adapter never filters an object out of a successful listing.

## Namespace observation

`DirectNamespace::observe` performs one recursive OpenDAL list and builds an
immutable `DirectSnapshot` complete over the exact key set returned by that
conforming list. A multi-page S3 list is not an atomic namespace snapshot:
external writers can make different pages describe different moments.
`DirectRevision` identifies the exact accepted result; it does not claim
transaction isolation.

The FUSE adapter accepts one snapshot at startup and reuses it for LOOKUP,
GETATTR and directory enumeration. `SIGHUP` observes and atomically replaces
the cached snapshot, then invalidates known inodes and dentries. The adapter
does not run a background watch and does not recursively list for every
LOOKUP. Memory use is proportional to the accepted namespace and visited
directories.

Directories are synthesized from path prefixes. Read-only Direct directories
are mode `0555`, files are `0444`, and ownership is the mounting effective
uid/gid. File mtime comes from the accepted storage observation, normalized to
whole seconds because LIST and HEAD interfaces do not share sub-second
precision. Synthetic directories and observations without a timestamp use the
fixed Unix time `1600000000`. Size and kind come from the same accepted entry.

## Object pins and open descriptors

File identity includes the mapped path, kind, accepted storage pin, size and
whole-second mtime. The accepted pin is the real S3 version ID when available;
otherwise it is the ETag. `versionId="null"` and an empty version are not
immutable versions and are treated as absent.

A FUSE OPEN performs one `stat` for the selected key and compares it with the
accepted entry:

- a real accepted version must still be that version;
- otherwise ETag and size must match, and available mtimes must agree at
  whole-second precision.

ListObjectsV2 normally omits version IDs while a HEAD on a versioned bucket
returns one. This is valid: ETag, size and mtime establish that the object
still matches the accepted entry, after which the HEAD version strengthens the
descriptor pin. A mismatch returns `ESTALE` and requires `SIGHUP` before the
path can be opened again.

After OPEN, one descriptor retains its accepted size and timestamps, and every
read is pinned:

- a real version ID is sent with every range read;
- without a real version, the accepted ETag is sent as `If-Match` on every
  range read;
- an object with neither a non-empty ETag nor a real version cannot be opened.

Condition failure, a deleted pinned version or a short range response returns
`ESTALE`. The descriptor never performs an unconditional range read and never
combines ranges fetched under different pins. A versioned bucket can retain
bytes after key deletion; an unversioned ETag pin cannot, and becomes stale.

File replacement creates a different accepted pin and therefore a different
inode at the next snapshot. An old descriptor keeps the old inode, length,
timestamps and pinned ranges. A new LOOKUP after `SIGHUP` resolves the new
inode, so kernel attribute caching cannot rebase the old descriptor to the new
object.

## Read-only frontend behavior

`DirectNamespace` and `DirectSnapshot` expose observation, lookup, enumeration
and file open/read only. `DirectMutation` and the explicitly constructed
`DirectRuntime` are separate mutation APIs; constructing the read-only
namespace does not create staging state or grant writes.

The FUSE mount uses the kernel `ro` option. Writable or truncating opens,
CREATE, MKDIR, MKNOD, UNLINK, RMDIR, RENAME, LINK, SYMLINK, SETATTR, WRITE and
FALLOCATE fail with `EROFS`. Xattrs are unsupported with `EOPNOTSUPP`.
Negotiating and listing the namespace do not modify storage.

## Errors

| Direct error | FUSE error | Meaning |
| --- | --- | --- |
| `InvalidName` | `EINVAL` | A key cannot be mapped reversibly and losslessly |
| `Invalid` | `EINVAL` | A marker or file-prefix relationship is ambiguous |
| `NotFound` | `ENOENT` | The path or object no longer exists |
| `NotDirectory` | `ENOTDIR` | A file was used as a parent directory |
| `IsDirectory` | `EISDIR` | A directory was opened as a file |
| `Stale` | `ESTALE` | The object no longer matches the accepted or open pin |
| `Unsupported` | `EOPNOTSUPP` | The backend lacks a required Direct primitive |
| `Storage` | `EIO` | The storage request failed without a narrower mapping |

If a refresh observation is rejected, the previous snapshot is not replaced.
Namespace operations fail with `EIO` until a later `SIGHUP` succeeds, while
already open descriptors retain their pinned reads. The rejection and original
key are logged; a subsequent mount attempts a fresh observation and fails
startup if the namespace is still invalid.

## CLI and doctor

`yy --volume CONFIG capabilities` reports the admitted capability set, raw
storage primitives, and effective set without reading the namespace.
`yy --volume CONFIG mount MOUNTPOINT` performs admission before observing
storage. `yy --volume CONFIG status` reports a Direct observation summary
without creating staging state.

`yy --volume CONFIG doctor` validates admission, recursively observes the
namespace, and reports `namespace` health. Writable Direct Mount additionally
inspects its runtime profile read-only. Direct Sync additionally inspects its
SQLite binding and persisted pending, Unknown and conflict state read-only.
Doctor does not create a staging path, staging database, metadata database,
publication lease or remote object, and does not run recovery.

## Limits

- Direct reads the `s3` scheme with the explicitly selected Amazon S3 or MinIO
  profile; it does not auto-detect arbitrary S3-compatible semantics.
- Recursive enumeration memory and request cost grow with namespace size.
- LIST pages are not an atomic cross-page snapshot.
- The selected service must return exact keys. A lossy LIST that omits hidden
  descendants cannot be made complete by guessing key names or by per-object
  probes, and such a service is outside the Direct support boundary.
- The current Silo/MinIO compatibility evidence does not qualify the
  file-prefix-alias namespace combination. Other ordinary namespaces remain
  testable on that endpoint, but its result must not be presented as a general
  lossless-namespace claim.
- Directory markers, invalid components and file-prefix collisions reject the
  complete view rather than being renamed by mutating storage.
- The current version rejects empty components; it does not define an
  additional mapping that changes path depth.
- Direct does not provide rename-stable identity, hard links, symlinks, xattrs,
  distributed locks, cross-client or cross-object transactions, version
  browsing, automatic garbage collection or Managed receipts.
- Non-Linux Direct FUSE and filesystem directory atomicity remain future
  integration work. The mutation library's file rename is explicitly
  non-atomic safe-save.
