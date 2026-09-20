# Specifications

This directory contains the maintained contracts for Apache OpenDAL™ YinYang.

| Specification | Status |
| --- | --- |
| [YinYang Format core](yinyang-format.md) | Current implementation: materialized versions and OpenDAL head publication. |
| [Authenticated content profile](content-profile.md) | Experimental range verification and trusted preparation; independent of publication authority. |
| [Transactional filesystem](transactional-filesystem.md) | Target contract: shared filesystem and content semantics with object-storage and metadata-service publication. Not implemented; persistent encoding decisions remain open. |

A specification describes the currently supported behavior, APIs, wire formats,
invariants, compatibility rules, and implementation boundaries. Specifications
evolve with the implementation and must be updated in the same change as the
contract they describe.

The transactional filesystem specification is an explicit design-ahead
exception. It defines the replacement contract without requiring compatibility
with the current format. Its normative requirements apply to implementations of
that target, not to the existing core. Keep its status and open encoding
decisions visible until implementation and conformance evidence justify changing
them. Object-storage publication is the first implementation target; both
publication modes are part of the target contract.

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
