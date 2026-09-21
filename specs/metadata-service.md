# Metadata service

**Status: experimental single-host authority and authenticated loopback RPC.**
This binding implements the shared [transactional filesystem contract](transactional-filesystem.md)
using SQLite metadata and the existing OpenDAL content profile. It does not
provide high availability, multi-host consensus, or a remotely exposed transport.

## Authority and storage

`MetadataService::create/open` binds one local SQLite database to one content
prefix. The database uses WAL, `synchronous=FULL`, and `BEGIN IMMEDIATE` for
publication. Its filesystem must honor SQLite locking and synchronization;
network-mounted database files are unsupported. Keep the database and its WAL
together; copying only the live main database is not a backup procedure.

The schema profile is `yinyang-sqlite-1`. The authority row retains filesystem
and root identities plus the latest revision. Indexed valid-time records retain
nodes, entries, receipts, and ordered changes. Each record has an inclusive
start and exclusive end sequence. Revision tokens are retained separately and
validated before reads. History is application data, not an expiring database
MVCC window. There is no history deletion or garbage collection.

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

## Shared API and content readiness

`Authority` supplies common observations, preparation, batch publication, and
frozen-request restoration. `Snapshot`, `Planner`, `Transaction`, receipts,
changes, and revision/scan tokens are shared with object mode.

In-process preparation carries trusted `DataStore` evidence and records
readiness durably. A client uploads through OpenDAL, then registers its content
descriptor. The service fully verifies an untrusted descriptor before recording
readiness. Publication and restart use that persisted evidence; they do not
download the file again. Credentials and prefix isolation must prevent external
writers from mutating immutable published content, as in object mode.

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
revision, read/scan records at a revision, register content, or commit frozen
requests. Responses carry observations, records, readiness, typed outcomes, or
typed errors. Maximum batch size is 4096; scans request at most 4097 records.
The server handles at most 32 active connections, each with a 60-second timeout.
This wire profile is experimental and is not a stable interoperability promise.

## Failure and recovery

Database contention before commit returns `Retryable`; replay the original
request, not a newly observed/replanned one. Failed commit acknowledgement and
lost/timeout transport responses return `Unknown(commit_id)`. A timeout does
not cancel or prove rollback of an in-flight durable transaction. Resolve via
a retained receipt or retry the exact frozen request. Absent receipt is not
proof that a delayed request cannot still commit.

Duplicate identity/digest returns the original receipt. Identity reused with a
different digest is invalid. Reopening the same database retains receipts,
readiness, changes, and historical revisions. No automatic failover or
checkpoint writer exists. Listener shutdown stops accepting new work; already
accepted requests may finish.

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
