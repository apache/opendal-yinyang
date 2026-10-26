# Shared Mount node state

**Status: experimental online runtime; platform installation is separate.**

`yinyang::mount::Mount` owns a dedicated staging directory and one staged file
version per NodeId. It uses the same Authority and durable FileHandle storage as
the library runtime. Object and service publication have identical semantics.
Do not mix private library handles into this staging directory.

`open_node` returns an access-checked reference to the shared node. Reads see
writes from other references immediately after local acceptance. Writes,
truncate, append and fsync are serialized per node; append chooses its offset
under the same lock. Different nodes do not share an I/O lock. Initial
materialization is serialized per NodeId by a short-lived open gate, and the
instance node map is not held during a remote observation or staging read.
The node lookup and gate registration share one critical section, so a
completion cannot slip between them and start a second materialization.
Success, failure and cancellation close the gate to wake every waiter for a
retry; a retry rechecks the node map and either joins the materialized node or
becomes the next single owner. Different nodes can therefore complete their
first opens independently. Refresh and reclaim use the map for lookup and
reference accounting; data-plane calls keep per-node locks. An incomplete
local open is never exposed and is discarded when staging is reopened. Both
read-only and writable references currently use full-file staging.

Dropping a reference performs no I/O and never publishes or deletes data.
`reclaim` rejects referenced, pending, frozen or failed nodes. Clean unreferenced
nodes can be removed. Restart reclaims disposable clean observations and recovers
pending/failed nodes by stable identity, preserving the original frozen request.
Multiple retained versions of one node are rejected rather than guessed or
silently merged. No distributed lease or writable anonymous inode is provided.

`fsync` on a reference publishes the node and reports the real remote outcome.
Instance-wide `fsync` visits all nodes, continues after individual failures, and
returns the first failure. Every failure remains in runtime status. A successful
local write or an OS close that cannot report errors is not remote completion.

`flush` is the named publication attempt for a frontend close path that can
report errors. It performs the same conditional publication as `fsync` and
never reports success before the remote result is durable. Dropping or
releasing the reference is different: it performs no I/O, publication or
staging deletion. Pending, frozen and failed states remain recoverable, and
`reclaim` rejects them. A frontend whose release callback cannot report errors
must not use release as an implicit flush; if its kernel omits a reportable
flush, the bytes remain staged rather than being acknowledged as remote.

`node_status` observes the shared staged metadata for one NodeId without
opening another reference. It reports the same staged length and
pending/frozen/error state used by reads, so attribute paths do not have to
materialize or refresh a file merely to inspect its accepted local state.

`refresh` replaces a clean staged observation with a fresh remote version for
all references. It rejects pending, frozen and failed states; it never silently
rebases writes. Failure to fetch leaves the old version available. Interrupted
refresh may leave a disposable clean observation for restart cleanup.
There is no automatic refresh timer or implied kernel invalidation. An adapter
must coordinate OS caches outside runtime locks before exposing refreshed bytes.
An adapter using uncached I/O can refresh clean nodes explicitly. No OS callback
runs under a runtime lock.

Namespace operations retain the conditional Runtime contract, including atomic
replace/no-replace rename and typed failure. A moved node keeps its shared state;
removed nodes retain staged reads but cannot publish dirty content. Namespace
conveniences do not promise cross-process frozen-request replay.

Shared contract tests cover both publication authorities, reference-local write
permissions, cross-reference visibility, atomic append, close-one/keep-writing,
explicit refresh, retained conflicts, and original Unknown request recovery.
These tests are not native Linux, Windows or macOS mount acceptance.
