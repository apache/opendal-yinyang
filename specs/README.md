# Specifications

This directory contains the current, maintained contracts for Apache OpenDAL™
YinYang.

- [YinYang Format core](yinyang-format.md)
- [Directory publication and restoration](directory-transfer.md)

A specification describes the currently supported behavior, APIs, wire formats,
invariants, compatibility rules, and implementation boundaries. Specifications
evolve with the implementation and must be updated in the same change as the
contract they describe.

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
