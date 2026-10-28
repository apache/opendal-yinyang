# Linux cross-version compatibility acceptance

`cargo x test-compatibility` runs the object, authenticated service, Mount
staging, Managed Sync and directory-journal transitions against the immutable
fixtures in `tests/fixtures/compatibility/fd1338c`. The fixtures were produced
by the release `yy` binary from
`fd1338c2f924a90e9da752b9158e748d1d3acaf4`; `manifest.json` records the producer
binary SHA-256 and every payload hash. Current code is never used to regenerate
that corpus.

The command requires:

- Linux;
- the isolated `YINYANG_S3_*` settings used by the other real S3 suites;
- `YY_LEGACY_BIN` naming the release `yy` binary built from `fd1338c`.

MACOS and Windows runs do not skip a Linux acceptance stage inside a
purportedly completed Linux run: `test-compatibility` fails before running any
test when the environment or legacy binary is missing.

The suite proves both reader directions where the supported profiles permit
them:

- current processes reopen and restore `fd1338c` object history, pending
  staging bytes, service SQLite authority, Sync state, backup packages and
  committed publication journals;
- actual `fd1338c` processes are held at the object `.yinyang/head` commit
  boundary to create frozen directory, Mount and Sync requests; current
  processes reopen and retry the original CommitId/digest/bytes while newer
  local work remains a separate pending edit;
- `fd1338c` processes read state written by the current binary;
- unknown or malformed persistent profiles are rejected at their format
  boundary, and copied local or remote state is byte-identical after rejection.

The frozen fixture includes only SQLite backup images, never live `-wal`,
`-shm` or process lease files. The checked-in profile images are those emitted
by `fd1338c`: stage-2, sync-2, directory-replica-1, directory-1 and
`YYBK0001`. Legal stage-1/sync-1 upgrades and older replica `ALTER TABLE`
shapes are synthetic format regressions owned by the local-state suite; they
are not labeled as payloads produced by `fd1338c`.

`cargo x test` still runs platform-neutral tests and manifest integrity checks.
It does not replace `cargo x test-compatibility`.
