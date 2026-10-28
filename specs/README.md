# Specifications

This directory contains the maintained contracts for Apache OpenDAL™ YinYang.

| Specification | Status |
| --- | --- |
| [Compatibility](compatibility.md) | Supported persistent and wire profile matrix, admission, and transition limits. |
| [YinYang object profile](yinyang-format.md) | Experimental indexed transactions and OpenDAL head publication. |
| [Directory transfer](directory-transfer.md) | One-shot local directory publication and restoration. |
| [Authenticated content profile](content-profile.md) | Experimental range verification and trusted preparation; independent of publication authority. |
| [Metadata service](metadata-service.md) | Experimental durable SQLite authority and authenticated loopback protocol. |
| [File-operation runtime](file-runtime.md) | Pinned Managed handles, durable staging, conditional fsync, and recovery. |
| [Mount runtime](mount-runtime.md) | Instance-local shared nodes, explicit refresh, remote fsync, and retained failures. |
| [Direct volumes](direct.md) | Reversible key mapping, storage-native object pins, cached observations, and Linux FUSE reads. |
| [Direct conditional mutations](direct-mutations.md) | Conditional object publication/delete, durable whole-object staging, safe-save namespace mutation, and local state format. |
| [Direct Sync](direct-sync.md) | Finite ordinary-directory reconciliation over storage-native Direct pins and conditional mutations. |
| [Sync runtime](sync-runtime.md) | Experimental content and namespace coordinator against an explicit remote baseline. |
| [Linux Managed Sync](linux-sync.md) | The `yy ... sync` ordinary-directory CLI: admission, exit codes, offline retained-byte export, and the `yinyang.sync.status.v1` document. |
| [Linux operations](linux-operations.md) | Health diagnostics, user-service lifecycle and installed-prefix state boundaries. |
| [Managed Sync acceptance](managed-sync-acceptance.md) | Real-process Linux acceptance for ordinary-directory reconciliation, conflicts, artifacts, and recovery. |
| [Native bridge](native-bridge.md) | In-process ownership and private adapter request semantics. |
| [Experimental macOS frontends](macos-frontends.md) | Isolated FSKit and File Provider prototypes; not ordinary CLI admission. |
| [Volume capabilities](volume-capabilities.md) | Configuration, four-layer admission, Managed write policy, and CLI status. |
| [Linux FUSE frontend](linux-fuse.md) | Experimental writable and read-only Managed mount with native kernel acceptance. |
| [Transactional filesystem](transactional-filesystem.md) | Shared target contract. Object-storage and metadata-service bindings implemented experimentally. |

A specification describes the currently supported behavior, APIs, wire formats,
invariants, compatibility rules, and implementation boundaries. Specifications
evolve with the implementation and must be updated in the same change as the
contract they describe.

On 2026-10-27,
[PR #83](https://github.com/apache/opendal-yinyang/pull/83),
[PR #84](https://github.com/apache/opendal-yinyang/pull/84), and
[PR #85](https://github.com/apache/opendal-yinyang/pull/85) established the
Linux Managed Mount and its native acceptance suite. All 22 real-mount
scenarios passed on an isolated Ubuntu 24.04 AWS host and in GitHub Linux CI,
using both publication authorities. See the
[acceptance guide](../docs/linux-fuse-acceptance.md). This evidence does not
make the frontend a release-supported combination.

The transactional filesystem specification is an explicit design-ahead
exception. It defines the replacement contract without requiring compatibility
with the legacy format. Its normative requirements apply to both bindings;
the linked implementation profiles describe the supported subset and byte
encoding. Keep experimental status and remaining evaluation decisions visible
until conformance evidence justifies changing them. Object-storage publication
is implemented first; both publication modes remain part of the target contract.

RFCs in [`../rfcs`](../rfcs) preserve design decisions and their historical
context. An RFC may change while it is under review, but its file becomes
immutable once merged into `main`. Correct or supersede an accepted RFC with a
new RFC rather than editing the historical document.

A specification may cite RFCs for rationale, but it must be self-contained and
is authoritative for current behavior. Keep discussion, rejected alternatives,
and decision history in RFCs instead of specifications.

When the implementation, tests, and a specification disagree, first establish
the intended current contract. Update the implementation, tests, and
specification together as needed; do not repair the mismatch by rewriting a
merged RFC.
