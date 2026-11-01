# Metadata service

**Status: experimental single-host authority and authenticated loopback RPC.**
This binding implements the shared [transactional filesystem contract](transactional-filesystem.md)
using SQLite metadata and the existing OpenDAL content profile. It does not
provide high availability, multi-host consensus, or a remotely exposed transport.

## Authority and storage

`MetadataService::create/open` binds one local SQLite database to one content
prefix. The database uses WAL, `synchronous=NORMAL`, and `BEGIN IMMEDIATE` for
publication. A successful preparation or commit runs a FULL WAL checkpoint
before returning. Its filesystem must honor SQLite locking and
synchronization; network-mounted database files are unsupported. Keep the
database and its WAL together; copying only the live main database is not a
backup procedure.

The schema profile is `yinyang-sqlite-2`. The authority row retains filesystem
and root identities plus the latest revision. Indexed valid-time records retain
nodes, entries, receipts, and ordered changes. Each record has an inclusive
start and exclusive end sequence. Revision tokens are retained separately and
validated before reads. History is application data, not an expiring database
MVCC window. There is no history deletion or garbage collection.

Before changing the SQLite journal mode or creating tables, opening checks the
schema profile, the single authority profile row, the retained manifest, the
required tables, and each table's exact column-name set. An older or unknown
profile returns `Unsupported`; a current identifier with an incompatible schema
or manifest returns `Corrupt`. Neither path modifies the database. Maintained
inserts name columns explicitly, so column order is not part of the schema
signature. This admission is a format check, not a scan of all historical
records.

Writers serialize at the database boundary, but revalidate original logical
predicates through the same transaction evaluator as object publication.
Disjoint name slots and file states can both succeed from one observation;
membership scans, file state, emptiness, and ancestor predicates detect real
conflicts. Every accepted batch member's namespace delta, receipt, and changes
commit atomically. Members share a revision and have ordered cursor ordinals.
No publication rewrites a complete object snapshot.

An immutable `.yinyang/head` marker contains Borsh
`([u8;8] = YYSERV01, filesystem: [u8;16], root: [u8;16])`.
Creation conditionally installs it; opening validates it. Object-mode readers
reject this marker. Service commits neither replace it nor consult it as a
latest checkpoint. A service outage cannot authorize object-mode writes.
The database is the sole authority and is required for history and receipts.
An unknown marker profile is `Unsupported`; malformed `YYSERV01` bytes are
`Corrupt`; a valid marker naming another filesystem or root is `Invalid`.

## Shared API and content readiness

`Authority` supplies common observations, preparation, batch publication, and
frozen-request restoration. `Snapshot`, `Planner`, `Transaction`, receipts,
changes, and revision/scan tokens are shared with object mode.

`Authority::retained_revisions_page` enumerates the durable revision table in
strict token order. A page is limited to 256 revisions and uses the last
returned revision as its exclusive continuation. Embedded inspection opens the
existing SQLite database read-only with `query_only` enabled; it does not
migrate, recover, take the writer transaction, or infer retained revisions from
content packs. `ServiceClient` exposes the same bounded page contract.

In-process preparation carries trusted `DataStore` evidence and records
readiness durably with an explicit checkpoint. A client uploads through OpenDAL,
then registers its content descriptor. The service fully verifies an untrusted
descriptor before recording readiness. Publication and restart use that
persisted evidence; they do not download the file again. Credentials and prefix
isolation must prevent external writers from mutating immutable published
content, as in object mode.

Trusted fixed-offset updates have a separate path. `Authority::prepare_overwrite`
accepts an exact `FileVersion` and bounded `ContentEdit` ranges with unchanged
logical length. `Authority::prepare_update` additionally accepts a preserved
base prefix and final length; it retains only base bytes below that prefix,
zero-fills the gap to the final length, and overlays the bounded edits. The
embedded service applies either operation through its `DataStore`, verifies the
new pack, and records durable readiness. The authenticated client does not
register or import the whole result. Instead, its preparation request names the
selected revision, NodeId, operation parameters, and edits; the server reopens
that exact published base from SQLite metadata and applies the update itself.
An unknown or unretained revision, missing or non-file NodeId, or different
service binding fails before content payload reads. A retained older revision
is a valid base; its original file predicates determine whether publication
succeeds.

The request is capped at 4096 edits and the existing 16 MiB RPC frame.
`prepare_update` additionally rejects a final length whose generated zero and
affected-unit work exceeds the shared 4096-unit budget before payload reads.
Callers use `update_is_bounded` with the same rule before selecting the
complete-source preparation fallback. Unchanged published extents retain their
existing readiness; this path verifies affected proof paths and the newly
written pack, not every unchanged extent.

`Transaction::to_bytes` freezes the original observation, identity, predicates,
and mutations. The experimental Borsh envelope is
`(YYPLAN01, filesystem[16], commit_id[16], base_revision[24], digest[32], conditions, mutations)`.
Conditions are key/optional-value pairs. Mutation encodings reuse the logical
canonical transaction tags and record encodings. Physical prepared descriptors
are carried for file content; they do not enter the canonical logical digest.
Restoration validates the original retained snapshot, derived node identities,
all mandatory predicates, and digest. A caller cannot remove a condition to
bypass concurrency checks. An untrusted serialized request is not preparation
evidence. Object-mode restoration verifies referenced content through import.

`YYPLAN01` is the supported frozen-request profile. Analysis of an unknown plan
identifier returns `Unsupported` before observing the authority, importing
content, or attempting publication.

## Protocol and security boundary

`yy serve --database PATH --listen 127.0.0.1:7447` opens or creates the authority.
Storage uses the existing `YINYANG_S3_*` settings. Set
`YINYANG_SERVICE_TOKEN` to a random secret with 32–1024 bytes; do not place it in
command arguments. `ServiceClient::connect` checks the content-prefix binding.

The initial transport accepts loopback addresses only, including IPv6 loopback.
It is authenticated but not encrypted. Use it only among trusted local
processes; it has one full-authority token, no per-user authorization, no TLS,
and no supported public/network exposure. Tokens are omitted from client Debug.
A future remote deployment requires a separately specified secure transport.

Each TCP connection carries one request/response. Frames are a big-endian u32
length followed by Borsh, bounded to 16 MiB. Request fields are protocol version
1, token string, and a tagged command. Commands observe latest/specific
revision, read/scan records at a revision, register content, prepare same-length
or length-changing trusted updates from an exact published revision and NodeId,
enumerate retained revisions, or commit frozen requests. Responses carry
observations, records, readiness, prepared descriptors, typed outcomes,
bounded revision pages, or typed errors. Maximum commit batch size, overwrite
edit count, update edit count, and update generated-unit count are 4096; scans
request at most 4097 records; history requests return at most 256 revisions.
The server handles at most 32 active connections, each with a 60-second
timeout. This wire profile is experimental and is not a stable interoperability
promise.

Protocol version 1 is the only supported request version. The server checks the
token first, then returns `Unsupported` for a validly authenticated unknown
version before dispatching or decoding a command against storage. Clients and
servers that implement different versions fail explicitly rather than
negotiating down or sharing an authority.

New command and response variants are appended to the version 1 tagged enums so
existing variant tags do not change. The retained-history command and response
follow the overwrite and update variants. Older servers do not understand the
appended overwrite, update, or history commands and therefore require a
matching server binary for these paths. Existing
register/observe/read/scan/commit traffic retains its prior encoding.

Error responses retain the existing tag order: Invalid=0, Corrupt=1,
NotFound=2, AlreadyExists=3, Unsupported=4, Storage=5 and Io=6. Additional
domain tags are InvalidName=7, NotDirectory=8, IsDirectory=9, NotEmpty=10 and
PermissionDenied=11. Diagnostics remain strings. Clients reject unknown tags;
deploy matching client/server versions for these new categories. This extends
error reporting, not the persisted metadata or transaction encoding.

## Failure and recovery

Database contention before commit returns `Retryable`; replay the original
request, not a newly observed/replanned one. A successful SQLite `COMMIT` is
followed by a WAL checkpoint before local success is reported. A failed commit
or checkpoint acknowledgement and lost/timeout transport responses return
`Unknown(commit_id)`. A timeout does not cancel or prove rollback of an
in-flight durable transaction. Resolve via a retained receipt or retry the
exact frozen request. Absent receipt is not proof that a delayed request cannot
still commit.

Duplicate identity/digest returns the original receipt. Identity reused with a
different digest is invalid. Reopening the same database retains receipts,
readiness, changes, and historical revisions. No automatic failover or
checkpoint writer exists. Listener shutdown stops accepting new work; already
accepted requests may finish.

For a supported quiesced backup, freeze service clients and create a consistent
SQLite backup with the SQLite backup API (for example
`sqlite3 DB ".backup DEST"`), then copy the complete content prefix. The
service database and content marker must be restored together. See
[Linux backup and isolated recovery](../docs/linux-backup-recovery.md).

## Verification and operating limits

Shared integration tests run the same predicate and snapshot suite against
object publication, embedded service, and authenticated RPC. Fault injection
covers dropped and delayed acknowledgements, independent database connections,
content-readiness rejection, omitted predicates, and restart. The S3 daemon
test kills and restarts the real process against durable MinIO content.

SQLite indexes provide incremental record access, but historical query cost
depends on retained versions and query ranges; this profile does not promise
the object B+ tree's page bounds. SQLite's single writer is a throughput limit.
There is no benchmark-based production capacity claim or stable storage
migration support.

## Compatibility

The supported service profiles are `yinyang-sqlite-2`, `YYSERV01`, `YYPLAN01`,
the shared `YYFILE02`/`YYHASH02` content profile, and loopback RPC version 1.
There is no automatic schema upgrade or downgrade, no object-authority fallback,
and no cross-profile read of local preparation evidence. A same-profile process
restart retains published history, receipts, frozen requests, and readiness.

Current-profile content and transaction vectors under
[`format`](../crates/core/tests/fixtures/format) are consumed directly by the
tests. See [Compatibility](compatibility.md) for the reader/writer matrix and
unsupported-transition behavior.
