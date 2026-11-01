# Direct conditional mutation runtime

**Status: experimental library runtime; FUSE and Sync admission are separate
integrations.**

Direct remains a storage-native namespace. Its mutation runtime does not create
Managed NodeIds, content proofs, transactions, commit receipts, publication
leases or cross-object atomicity. The stateless mutation seam can be used
without local state. The durable runtime adds whole-object local generations
and mutation intents for writable Mount consumers.

## Keys, pins and preconditions

`DirectKey` is a validated whole-object key. `DirectKey::from_storage_key`
accepts a key returned by storage. `DirectKey::from_path` reverses the canonical
Linux-name mapping and rejects non-canonical percent escapes, decoded `/`, empty
components and names above the mapping limits.

`DirectObjectPin` contains the optional storage version, exact ETag, complete
size and whole-second modification time. It records either response metadata or
one later observation; callers must keep their provenance distinct.

Conditional replacement, create, copy and delete use only these conditions:

- `Absent`, lowered to `If-None-Match: *`;
- `Etag(exact)`, lowered to `If-Match`.

`*`, `null` and empty ETags are not concrete preconditions. Direct does not
claim version-only compare-and-swap when the selected backend does not advertise
and enforce a version condition. S3's ETag condition has the normal ABA limit:
a later object with the same ETag is indistinguishable. A version ID in an
observation is diagnostic evidence unless it is the condition actually enforced
by the backend.

## Stateless mutation seam

`DirectMutation::open(DirectNamespace)` creates no database, staging directory
or other local state. It exposes:

```text
publish_file(key, immutable_source, precondition)
delete(key, precondition)
```

`publish_file` streams the caller-owned source in bounded chunks through a
conditional writer. The condition is present at multipart completion. A local
read error aborts completion; a completion error is classified from the backend
response rather than from expected content.

The source path is caller-owned accepted data. `DirectMutation` checks the
length observed during its read, but that check is not an immutability proof.
Direct Sync and the durable runtime must freeze their own accepted artifact
before calling this seam.

`DirectMutationOutcome` is:

| Outcome | Meaning |
| --- | --- |
| `Applied { accepted }` | The response confirmed the operation. Write/copy metadata populates `accepted`; conditional delete has no response pin. |
| `Conflict { observed }` | The supplied condition was false. |
| `Unknown { observed, message }` | The request may have reached storage and no definitive outcome is known. |
| `Unsupported { message }` | The selected backend lacks the native primitive. |

`DirectObservation` distinguishes `Present(pin)`, `Missing` and
`Unavailable(message)`. `Missing` is a successful exact observation of absence;
`Unavailable` is not permission to update a baseline or infer deletion. A
definitive 404 on a conditional delete is retained as `Conflict` with
`Missing`, including S3's delete-marker case.

Applied identity comes only from successful response metadata. A later HEAD
does not replace that accepted pin. Unknown is never converted to success
because current bytes equal the intended bytes. Unknown is resolved only by
evidence such as the unchanged original precondition, an exact accepted
version/ETag observation, or an explicit observed-desired decision; equality
alone is not commit proof. Safe automatic re-dispatch after Unknown relies on
the selected backend preserving linearizable conditional mutation semantics;
the backend qualification must establish that property.

## Storage binding

Writable runtime state is bound to:

```text
DirectStorageBinding::new(
  scheme, profile, bucket, canonical_root, canonical_endpoint_or_provider_default
)
```

The type accepts only `scheme=s3`, `profile=amazon-s3|minio`, one bucket, a root
without control characters, and a non-secret endpoint without userinfo, query
or fragment. It preserves the supplied canonical endpoint case, path and
trailing slash behavior. Credentials must never be placed in this value.

The binding digest is BLAKE3 over the domain
`yinyang direct storage binding v1` followed by length-prefixed scheme, profile,
bucket, root and endpoint. `namespace_id()` returns the digest; `description()`
returns the non-secret diagnostic fields. Reusing one state directory with a
different binding is `Invalid` before crash recovery or mutation.

## Durable local profile

The profile is `yinyang-direct-runtime-2`. A writable runtime requires a
dedicated staging directory:

```text
runtime.db
runtime.db-wal / runtime.db-shm       SQLite coordination as needed
runtime.lock                          exclusive process lease
objects/<16-byte-object-id>.data      shared local object generation
inputs/<16-byte-mutation-id>.data     immutable accepted dispatch input
```

`runtime.db` uses SQLite WAL and `synchronous=NORMAL`. Its exact tables are:

- `authority(singleton, profile, binding, binding_diagnostic)`;
- `objects(id, key, record)`;
- `mutations(id, record)`.

`record` columns are Borsh values. Object data paths and mutation input paths are
derived from their 16-byte IDs. Data and metadata files use private permissions.

`ObjectRecord` fields, in order, are:

```text
id, key, path, parent, name, existed_remote, precondition, state,
generation, durable_generation, length, digest, accepted, observed,
pending_mutation, error
```

Object states are `Clean=0`, `Staged=1`, `Pending=2`, `Conflict=3`,
`Unknown=4`, `Failed=5` and `Discarded=6`.

`MutationRecord` fields, in order, are:

```text
id, kind, state, generation, source, destination, input_length,
input_digest, accepted, source_observed, destination_observed, error
```

Mutation kinds are `Publish=0`, `Create=1`, `Delete=2`, `Copy=3` and
`Rename=4`. Mutation states are `Staged=0`, `Dispatching=1`, `Applied=2`,
`Conflict=3`, `Unknown=4`, `DestinationApplied=5`,
`PendingSourceCleanup=6`, `Failed=7` and `SourceDispatching=8`.

`PinRecord` stores optional version, optional ETag, size and optional
whole-second time. `ObservationRecord` is `Present`, `Missing` or
`Unavailable`. `EndpointRecord` stores ObjectId, raw key, mapped path, parent,
name and precondition. The exact Borsh enum and struct field order is part of
this profile.

There is no migration, downgrade or mixed-profile writer support. An existing
nonempty database is read-only preflighted before creating directories or a
lease. Missing authority, an older or unknown profile, missing or extra tables
or columns, a missing or duplicate profile row, or binding mismatch rejects
without DDL, journal-mode change or recovery. Column order is not part of the
schema signature because maintained inserts name their columns. An absent or
zero-length database may be initialized. A partial first initialization is
therefore not silently adopted.

Mutation journal rows that make an external write or delete recoverable are
checkpointed before that storage request. This includes `Dispatching`,
`SourceDispatching`, object `Pending`, and the paired frozen mutation input
records. Completion rows may remain at the normal-commit durability level until
the next explicit barrier or process exit because a restart safely converts an
unacknowledged dispatch state to `Unknown`.

On reopen, a stored `Dispatching` mutation becomes `Unknown`; a stored
`SourceDispatching` rename becomes `PendingSourceCleanup`; and an object left
`Pending` becomes `Unknown`. Startup performs no remote retry. The profile has
no automatic garbage collection; completed inputs and readable conflict bytes
remain until an explicit later policy is specified.

`DirectRuntime::inspect(path, Option<&DirectStorageBinding>)` performs
non-mutating doctor admission. It returns `Missing` when the path is absent and
`Uninitialized` when the directory exists without a nonempty `runtime.db`. It
does not create files, acquire `runtime.lock`, change journal mode, run startup
recovery, or contact storage. For nonempty state it validates the profile,
exact schema, Borsh records and optional binding, then reports object and
mutation counts by state plus the stored binding digest/description. Unknown
profiles, partial schemas, corrupt records and a mismatched supplied binding
fail inspection instead of being reported as ready.

## Shared object generations

All handles for one raw key share local bytes. A writable open first materializes
the pinned whole object. A read-only handle is either a pinned remote descriptor
or a view of the same local generation when one exists.

Every process-local `ObjectEntry` receives a random `DirectObjectId` once. All
handles sharing that entry return the same `object_id()` even when the entry is
rebound to a newer DirectId; independently retained old/new pin entries return
different ObjectIds. The value is coordination metadata, not durable state.
`DirectFileHandle::id()` may therefore change on a clean-pin rebind; callers
that cache process-local views by shared entry must use `object_id()`, not the
snapshot DirectId.

Every successful local write, append or truncate:

1. changes the local data file;
2. calls `sync_data`;
3. increments the local generation and records the acknowledged length;
4. commits the object record before returning success.

The applied and local generations are separate. A process restart can therefore
retain acknowledged dirty bytes without treating them as remotely applied.
Larger bytes found beyond the last committed record are not acknowledged and
are discarded on reopen. A missing shorter data file is corruption.

`flush` first freezes the exact current generation into `inputs/<id>.data` and
records its length and BLAKE3 digest. It then commits and checkpoints
`Dispatching` before the network call. A newer local write cannot alter that
frozen input. `flush` on a read-only handle is `Unchanged`; Drop and release
never publish.

The writable open/create creates an empty local generation. No remote object
exists before its first flush. `discard_local` removes only a never-dispatched
local-only name; an unresolved or possibly dispatched create is rejected before
effects.

`resolve_object(id, KeepLocal)` explicitly rebases retained local bytes onto
the currently observed ETag or absence and stages a new conditional generation.
`resolve_object(id, KeepRemote)` returns Unsupported without an exact
observation, and otherwise drops only the local overlay without a storage
request. Both decisions are limited to `Conflict`, `Unknown` or `Failed`
generations; neither claims that an earlier ambiguous request committed.

`pending_objects` reports local-only, staged, conflicted and Unknown objects
with stable ObjectId, raw key, mapped path, parent, name, length, remote-existed
flag, precondition, accepted/observed evidence and pending mutation. Clean
observations from an old pin are not reused for a new snapshot.

After a successful own write, a clean retained entry may be promoted when the
new snapshot's HEAD pin exactly matches its accepted response pin. Promotion
updates the entry ObjectId to the new DirectId instead of creating a second
clean entry for the same key. Existing writers and readers keep sharing the same
local bytes; only FUSE-owned descriptor attributes remain pinned separately. An
unrelated external successor does not match the accepted pin and does not reuse
the old clean bytes.

## Unlink

`unlink_file` requires the observed ETag and issues conditional delete. It has no
stat-plus-unconditional-delete fallback. A clean successful unlink removes the
local clean record; retained open descriptors may keep already-open bytes.

If a dirty handle exists, its bytes remain readable and the object becomes
`Conflict` with `observed=Missing`. A later flush reports Conflict and cannot
recreate the name. A `Unknown` delete keeps the local generation and is retried
only after exact observation: missing source establishes the observed desired
state, unchanged source ETag permits a conditional retry, and changed source
conflicts.

## Copy and safe-save rename

Copy freezes source bytes from its exact pinned descriptor, conditionally
publishes the destination against `Absent` or the observed destination ETag, and
leaves the source unchanged.

Rename is a durable two-step safe-save sequence, not a POSIX atomic rename:

1. freeze the pinned source bytes and persist source/destination conditions;
2. conditionally publish the destination;
3. retain `DestinationApplied` with the response pin;
4. persist `SourceDispatching`, then conditionally delete the source.

`PendingSourceCleanup` means the destination was applied but source deletion is
not definitively complete. Retry first observes the destination. If its current
identity no longer matches the accepted response pin, the mutation becomes
`Conflict` and the source is not deleted. If the destination still matches and
the source is already missing, the result is Applied as an observed desired
state, not proof of the original request. An unchanged source ETag may issue the
conditional delete. Any other source state conflicts.

Direct does not advertise cross-object atomic rename, directory rename,
`RENAME_EXCHANGE`, whiteout, recursive delete or Managed rename receipts.
Directory/exchange/whiteout callers must reject before invoking this runtime.

## Errors and integration boundaries

Conflict, Unknown, Missing, response metadata and retained local bytes are
normal typed results, not generic storage failures. Malformed local profiles or
records use `Corrupt`; I/O and storage failures use `Storage` unless a narrower
Direct error applies. FUSE admission,
writable volume configuration, CLI status, Direct Sync baselines and Sync
algorithm code remain separate owners. They must consume this runtime and the
stateless seam instead of implementing a second Direct publication engine.
