# Repository instructions for agents

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the
[design principles](specs/design-principles.md) before planning or modifying
this repository. Every pull request answers the principles' review checklist.

## Repository workflow

Use `cargo x` as the source of truth for repository tasks. Run `cargo x --help`
before choosing a command and `cargo x <command> --help` for command-specific
usage.

Before handing work back, run:

```shell
cargo x lint
cargo x check
cargo x test
cargo x licenses
```

Keep commits focused and use semantic commit and pull request titles such as
`feat:`, `fix:`, `docs:`, `build:`, or `ci:`.

## Documentation lifecycle

- `rfcs/` records design decisions and their historical context. An RFC may be
  edited while its pull request is under review. Once the RFC is merged into
  `main`, treat that file as immutable: do not edit, rename, move, or delete it.
  Correct or supersede an accepted RFC with a new RFC that links to the earlier
  decision.
- `specs/` is the source of truth for current supported contracts and
  implementation details. Update the relevant specification in the same change
  whenever an API, behavior, wire format, invariant, compatibility rule, or
  implementation boundary changes.
- Specifications describe the current contract. Keep decision history,
  rejected alternatives, discussion, and migration rationale in RFCs. A
  specification may cite RFCs for rationale, but it must remain self-contained.
- When code, tests, and a specification disagree, establish the intended current
  contract before editing. Fix the implementation or update the specification
  in the same change; never rewrite a merged RFC to make it match current
  behavior.

## Important notes

- Minimum Rust version is 1.91.0, configured in `Cargo.toml`. The development
  toolchain tracks stable through `rust-toolchain.toml`.
