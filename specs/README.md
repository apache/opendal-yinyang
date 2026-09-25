# Specifications

This directory contains the maintained contracts for Apache OpenDAL™ YinYang.

| Specification | Status |
| --- | --- |
| [YinYang object profile](yinyang-format.md) | Experimental indexed transactions and OpenDAL head publication. |
| [Directory transfer](directory-transfer.md) | One-shot local directory publication and restoration. |
| [Authenticated content profile](content-profile.md) | Experimental range verification and trusted preparation; independent of publication authority. |
| [Metadata service](metadata-service.md) | Experimental durable SQLite authority and authenticated loopback protocol. |
| [File-operation runtime](file-runtime.md) | Pinned Managed handles, durable staging, conditional fsync, and recovery. |
| [Mount runtime](mount-runtime.md) | Instance-local shared nodes, explicit refresh, remote fsync, and retained failures. |
| [Sync runtime](sync-runtime.md) | Baseline-bound local edits, conditional publication, conflict retention, and exact retry. |
| [Native bridge](native-bridge.md) | In-process ownership and private adapter request semantics. |
| [Experimental macOS frontends](macos-frontends.md) | Native FSKit and File Provider integration boundaries. |
| [Volume capabilities](volume-capabilities.md) | Configuration, four-layer admission, read-only enforcement, and CLI status. |
| [Transactional filesystem](transactional-filesystem.md) | Shared target contract. Object-storage and metadata-service bindings implemented experimentally. |

A specification describes the currently supported behavior, APIs, wire formats,
invariants, compatibility rules, and implementation boundaries. Specifications
evolve with the implementation and must be updated in the same change as the
contract they describe.

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
