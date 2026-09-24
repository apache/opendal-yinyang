# Experimental macOS frontends

**Status: prototypes on macOS 27 / Xcode 27, not production volume support.**

The adapters link `yinyang-native` and use the same Authority, Mount and Sync
coordinators as library callers. `cargo x macos` builds a host and an FSKit
extension. Signing and OS registration are explicit operator actions, not build
side effects. Neither adapter initializes or reformats remote storage.

## FSKit Mount

The FSKit path resource is an explicitly selected directory containing
`volume.json`; durable staging is its `mount-state` child. The extension obtains
the security-scoped resource before reading either.

The volume uses core NodeIds internally and stable volume-lifetime numeric inode
IDs at the kernel boundary. Directory verifiers retain an immutable revision;
cookies are ordinal positions, not live-list offsets. At most 1024 enumerations
are retained; an evicted verifier returns ESTALE. The item identity map currently
lives until unmount and is not a large-tree memory scalability claim.

All filesystem callbacks serialize off the main thread. DataCacheHandler grants
`noCache`; clean reads refresh the shared Mount node, while dirty and unlinked
nodes retain staged bytes. This favors correctness over read performance and
does not claim mmap, kernel-cache invalidation or a distributed POSIX cache.
The macOS 27 read/write result objects carry updated attributes back to FSKit.

**Concurrent external length changes are not supported by this prototype.** On
the tested macOS 27 build, the kernel can retain the old EOF despite newer
attributes returned by read/get-attributes handlers. Data-cache invalidation alone
does not establish metadata coherence. Remount before consuming externally
resized files. This is an acceptance blocker for multi-client writable Mount;
the uncached data path alone is not evidence that this contract is satisfied.

Reads, writes, truncate, create, mkdir, unlink, rmdir, enumeration and atomic
replace rename use the real runtime. Volume synchronization publishes staged
content and propagates errors. The non-reporting close callback attempts fsync,
logs a failure and retains staged state; close is not an error-reporting durability
boundary. Recovery reopens the same staging directory and original frozen request.

Ownership and modes are synthesized (current uid/gid, files 0644 or 0755 according
to the core executable flag, directories 0755). Creation does not persist requested
permissions. Timestamps encode generation, not wall-clock times. Permission and
timestamp changes, links and extended attributes are unsupported. There is no
disk capacity claim. Unsupported metadata is not marked consumed.

There is no conflict-resolution UI. Retained failures and bytes remain in the
runtime staging journal; operators must not delete that directory to recover a
failed write.

## File Provider Sync

`cargo x macos --file-provider` also embeds a replicated File Provider extension.
The host registers generated UUID domains only after the operator selects an
isolated resource. Host and extension share `group.com.xuanwo.yinyang.prototype`.
Each domain stores a private copy of its native configuration and `sync-state`
under its UUID directory. Credentials never travel through `domain.userInfo`.
Removing a test domain retains this staging for recovery; the OS may remove its
downloaded test copies. Existing domains from other providers are not touched.

Item identity is the complete core NodeId. Content and metadata versions carry
an immutable observation revision. Full and aligned partial downloads pin that
revision for every chunk. Partial content is written at the original file offset
in an OS-provided temporary file. Cancellation stops between chunks and removes
unreturned downloads. The extension closes and synchronizes the file before its
completion callback transfers ownership to the OS. Crash-orphan temporary-file
cleanup has not been validated through the system lifecycle.

Only replacement content of existing files is writable. Directory creation,
deletion, rename, metadata mutation and conflict-resolution UI are unsupported;
item capabilities do not advertise them. Upload stages a baseline-bound Sync
edit and reports its accepted revision. Identical retries reuse the same edit;
conflicts preserve both local bytes and remote state. The completion applies to
the supplied OS version; it does not separately mark later local edits in sync.

Directory pages pin one revision. The working set recursively includes the whole
tree, including every materialized item. Page and change tokens bind domain and
container and stay below the platform's 500-byte limit. They contain revision
and ordinal offsets, never process-local references or a growing directory queue.
Restart rewalks immutable pages; large-tree performance is not a supported claim.
Change enumeration splits even a large commit across bounded callbacks while
advancing the core receipt cursor only after all of its node changes are delivered.
Malformed or foreign tokens expire rather than silently restarting midway.

While the extension is alive, a five-second poll of the authoritative revision
signals the working set and active enumerators. The host's Refresh action can
wake an idle domain. There is no background push delivery guarantee while the
extension is not running. File Provider local fsync remains an OS-local operation,
not a Mount-style remote durability acknowledgment.

Unsigned builds and real-core callback tests do not establish signed domain
registration, Finder hydration, OS upload scheduling, OS-driven cancellation or
extension restart acceptance. Those remain explicit platform validation gates.
