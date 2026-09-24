# Native in-process bridge

**Status: experimental private adapter protocol, not a stable public ABI.**

`yinyang-native` builds an rlib and static library. Native adapters call
`yy_native_call(input, length)` with borrowed UTF-8 JSON and copy the returned
NUL-terminated JSON before calling `yy_native_free` exactly once. The input must
remain readable for the entire call. Null and oversized inputs return errors;
invalid pointers remain a caller ABI violation. Responses have either `ok` or
`error` (typed runtime category, diagnostic and optional Unknown commit ID).
Rust panics are contained at the ABI boundary. Only that boundary permits unsafe
Rust; the core, runtime and request dispatcher remain safe Rust.

Connect returns an opaque session integer; calls and close use that integer.
Closing does not publish pending data. In-flight calls own their session until
completion; subsequent calls to a removed session fail Closed. Calls block the
foreign thread and must run off the main/UI thread. Each session serializes
requests. No native callback runs inside Rust and no daemon or network transport
is introduced by this bridge.

The native configuration explicitly selects an existing Managed S3 authority,
Amazon S3 or MinIO profile, absolute staging path, read-only policy and optional
authenticated loopback metadata service. It never creates or reformats remote
storage. Credentials are supplied by the containing app; configuration and
secrets must not be committed or printed. This experimental configuration does
not silently admit a previously rejected `yy --volume` frontend combination.

Mount sessions expose shared reads/writes, truncation, remote fsync, clean
refresh, release, namespace operations and atomic replace rename. Sync sessions
expose pinned version downloads and baseline-bound local replacement upload.
Mode-specific operations fail in the wrong mode. Native I/O is limited to 1 MiB
per call and the entire encoded request to 8 MiB; adapters split larger I/O.

Metadata identities are the complete 16-byte NodeId encoded as hexadecimal.
Revisions are the complete 24-byte revision. Directory pages carry an explicit
revision and ordinal offset; later pages reopen that immutable observation,
including after a process restart. Page size is 1..=256 and offset is bounded
to one million entries. Rewalking earlier pages costs linear work; this is a
correctness-first experimental cursor, not a scalability claim. Metadata reports
the shared staged size for open Mount files.

Change pages use the core revision and receipt ordinal, not process-local IDs.
The supplied revision must belong to this filesystem. A page returns complete
receipts and their before/after nodes; the next cursor advances only past those
receipts. An anchor with ordinal `u32::MAX` starts after its observed revision.
An exactly full page may require one final empty page to establish completion.

The ABI dispatches storage futures onto Rust worker threads with an explicit
4 MiB stack. Foreign callback threads only wait for the task; their small native
stacks are not used to poll the storage runtime.

Filesystem-native inode numbers, provider anchors, cancellation and system cache
coordination belong to the adapters. Native exposure requires its own signed
bundle and real OS acceptance; passing these bridge tests alone is insufficient.
