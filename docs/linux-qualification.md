# Linux qualification coverage

Status: experimental Ubuntu 24.04 x86_64 qualification. This page separates
continuous repository coverage from one-time host and backend evidence. A green
unit test or a successful process exit is not by itself operational
qualification.

## Continuous coverage

| Entry point | Environment | Contract exercised |
| --- | --- | --- |
| `cargo x test` | host-independent test runners | Rust unit and integration tests; ignored native tests are compiled but not executed |
| `cargo x test-linux-fuse` | Ubuntu, `/dev/fuse`, isolated Silo | Real `fuse.yinyang` mount, read-only range hydration, writable recovery, corruption, refresh, restart and lifecycle |
| `cargo x test-linux-direct` | Ubuntu, `/dev/fuse`, Python 3, isolated Silo | Read-only and conditional-writable Direct Mount, Direct Sync, raw inventory/bytes, uncertainty and restart boundaries |
| `cargo x test-linux-operations` | Ubuntu, `/dev/fuse`, isolated Silo | Installed `yy` layout, doctor diagnostics, object Mount and finite Sync, loopback service Mount |
| `cargo x test-linux-recovery` | Ubuntu, isolated Silo, `sqlite3`, passwordless sudo | Consistent authority backup/restore, retained pending and unknown work, changed artifacts, and bounded-tmpfs disk exhaustion |
| `cargo test --test linux_prefix_upgrade` | Ubuntu, isolated Silo, `YY_LEGACY_BIN`, `YY_UPGRADE_BIN`, `sqlite3` | Stopped installed-prefix transition from the `fd1338c` producer to a selected target with retained file, Sync and service state |
| `cargo test --test linux_stage3_upgrade -- --ignored --test-threads=1` | Ubuntu, isolated Silo, immutable `546f419` fixture, installed `YY_UPGRADE_BIN` | Genuine stage-3 sparse bytes, pending/frozen/Unknown identities, service WAL reopen and target publication |
| `.github/workflows/linux-trial.yml` | fresh Ubuntu 24.04 x86_64 runner, isolated Silo | Actual archive checksum/inventory/identity, immutable install, quiesced activation, installed bytes/doctor, recovery, and archive-target `fd1338c` plus stage-3 upgrades |

GitHub Actions runs the FUSE and installed-CLI checks in the `linux-fuse` job
after starting isolated Silo. It runs backup and recovery acceptance in the
`s3` job after installing `sqlite3`. The broad `s3_` test filter explicitly
skips the two operations tests that require `/dev/fuse`, so the non-FUSE job
cannot report an accidental green or fail through a missing device.

The separate trial-distribution workflow uses the verifier and installer
packaged inside the archive. Its installed archive smoke, isolated recovery and
installed-prefix upgrade use the produced `bin/yy`; `YY_UPGRADE_BIN` is the
shared native-acceptance selector and falls back to the Cargo test binary only
when unset. The installed-operations entrypoint also copies that selected binary
into its clean prefix and exercises a real writable `fuse.yinyang` mount,
remount and finite Sync for both publication authorities. The workflow records
the source and selected binary SHA-256 before those runs, and the archive-target
upgrade supplies the pending/frozen/Unknown/receipt/WAL recovery evidence for
the produced binary.

Run only the first-layer read-only FUSE scenarios when isolating frontend
changes:

```shell
cargo x test-linux-fuse --read-only
cargo x test-linux-fuse --read-only-range
```

## Explicit host qualification

These checks require infrastructure that hosted CI does not provide:

| Entry point | Additional prerequisite | Coverage boundary |
| --- | --- | --- |
| `cargo x test-linux-operations --systemd` | running systemd user manager, `/dev/fuse`, isolated Silo | install/start/restart/stop/uninstall templates for service, Mount and finite Sync |
| `cargo x test-linux-operations --amazon-s3` | real Amazon S3 credentials and bucket, `/dev/fuse` | installed CLI lifecycle against the named S3 backend |
| `python3 tests/linux_direct_real_s3.py` | task-owned versioned and never-versioned Amazon S3 buckets, temporary role, AWS CLI, `/dev/fuse` | read-only Direct pagination, reversible keys, version/ETag retention, conditional primitives and Linux FUSE lifecycle; a separate source-pinned run covers writable remount and finite Direct Sync |
| `cargo x bench-linux-cost --workload read-only-range` | matched source identities, release binary, isolated S3, `/dev/fuse` | bounded FUSE range latency, requests, payload and staging |
| `cargo test --test linux_prefix_upgrade` | isolated Silo, exact `fd1338c` and final-target binaries, `sqlite3` | stopped versioned-prefix replacement with pending/Unknown state, receipts and service WAL recovery |
| `tests/linux_operating_envelope_real_s3.py` | dedicated real Amazon S3 versioned bucket, `/dev/fuse`, `sqlite3`, scoped AWS CLI | bounded sustained Mount I/O, sampled RSS/state/history, restart and independent final-byte convergence |
| `tests/linux_stage3_real_s3.py` | one unversioned Amazon S3 bucket, installed archive binary, temporary role over stdin, `/dev/fuse` | one small Managed object: append, truncate, zero extension, shrink/extend, clean remount, exact bytes and `space report`, followed by exact-prefix cleanup |

The installed-prefix command accepts `YY_LEGACY_BIN` for the immutable
`fd1338c` producer, `YY_UPGRADE_BIN` for the selected target executable, and
optional `YINYANG_UPGRADE_SOURCE`/`YINYANG_UPGRADE_EVIDENCE` values for the
exact target source and JSON output path. When `YY_UPGRADE_BIN` is omitted,
the integration-test binary built from the current worktree is used. A release
binary rebuilt from the exact `fd1338c` commit in a different build environment
is accepted only with `YINYANG_UPGRADE_LEGACY_SOURCE` and
`YINYANG_UPGRADE_LEGACY_REBUILD=1`; the evidence records
`source-pinned-rebuild` instead of the fixture binary hash.

The systemd lifecycle remains an explicit qualified-host command. Hosted CI
does not enable linger, create a boot service, or change FUSE configuration to
manufacture a user session. Run it on a disposable Linux host with an already
running user manager and retain the exact binary, host, storage service and
command evidence.

The Amazon S3 run qualifies only the bucket, region, endpoint and credential
mode actually recorded. A loopback Silo run does not imply Amazon S3 behavior,
and this matrix does not claim another S3-compatible provider.

The bounded installed-target Amazon S3 trial accepts STS credentials only on
stdin:

```shell
YINYANG_QUALIFICATION_BUCKET="<unversioned-bucket>" \
YINYANG_QUALIFICATION_REGION="us-east-2" \
YINYANG_STAGE3_TARGET_YY="$prefix/current/bin/yy" \
YY_UPGRADE_RELEASE="<installed-release>" \
YINYANG_STAGE3_REAL_S3_OUTPUT="/var/tmp/stage3-real-s3.json" \
python3 tests/linux_stage3_real_s3.py
```

The helper removes inherited `YINYANG_*` and AWS endpoint overrides, uses the
`amazon-s3` storage profile without an endpoint override, keeps a unique
`stage3-real-s3/` prefix, and deletes only that prefix with the scoped AWS CLI.
It does not test the Silo fault gate or repeat the sustained operating-envelope
matrix.

The explicit real-Amazon-S3 Direct record is
[`linux-direct-amazon-s3.md`](linux-direct-amazon-s3.md). It qualifies only the
two listed buckets, default us-east-2 endpoint, temporary role and sampled
pagination/version behavior; it does not turn the loopback Silo suite into
Amazon S3 evidence.

The source-pinned real-Amazon-S3 Direct entrypoint run remains the
`f629ed207cce81f196b55915bdd1154a3269a79a` record with debug `yy` SHA-256
`3dfe4c573b2d3311e07d4bf7c3be225a21d8541eec6fc7dc02262638ce06b9a0`.
Its public boundary is recorded in
[`linux-direct-amazon-s3.md`](linux-direct-amazon-s3.md). The final integrated
source `17361970d1722ea3d60c5381bbb5323d11818184` did not repeat those Direct,
backend, or range workloads.

Final integrated source `17361970d1722ea3d60c5381bbb5323d11818184` passed the
repository gates, Rust 1.91 locked check, default release archive, actual
`fd1338c` installed upgrade with a 74,192-byte service WAL, the 2700-second
Managed operating envelope. Details, sampling gaps, limitations and public raw
JSON are recorded in
[`linux-operating-envelope.md`](linux-operating-envelope.md).

## Matrix

The initial qualified combination is:

- Ubuntu 24.04 x86_64;
- one local systemd user manager on a single host;
- object publication or authenticated single-host loopback service publication;
- Managed Mount and finite ordinary-directory Managed Sync;
- isolated MinIO-compatible Silo for deterministic fixture coverage of Managed,
  read-only and conditional-writable Direct Mount, and finite Direct Sync; this
  is not qualified conditional-DELETE backend coverage because Silo ignores
  `DELETE If-Match`;
- Amazon S3 as the independently qualified backend for read-only Direct and
  conditional mutation primitives on the explicitly recorded buckets;
- installed, stopped binary replacement only for the exact producer and target
  source pair recorded by installed-prefix evidence; arbitrary upgrade and
  downgrade remain unsupported.

The metadata service remains one authenticated loopback authority. This matrix
does not claim TLS, HA, network-safe service exposure, multi-host locking,
automatic Sync watching, or automatic garbage collection.

Deterministic writable Direct Mount and Direct Sync coverage uses isolated
loopback Silo. Silo ignores `DELETE If-Match`; deterministic injected `412`
responses and request-count checks cover the local fault path, but they are not
conditional-DELETE provider certification. The source-pinned Amazon S3
entrypoint evidence covers writable remount and finite Direct Sync at
`f629ed2`, while the Amazon S3 primitive record covers read-only Direct and raw
conditional requests. The accepted Core115 proof at `b630ef2` covers Managed
FUSE 27 tests, operations 1, Managed Sync 12, compatibility 8, recovery 4, and
Direct 30 passed plus 8 filtered tests. Subsequent final-source changes were
the doctor remediation text, documentation/specification text, harness
procedure and Python test-loader registration; they did not change native
contracts. Those Direct/backend/range workloads were not repeated at
`1736197`. Other
S3-compatible services remain outside the matrix unless they satisfy exact
recursive LIST, conditional ETag mutation and the tested version/range
behavior.

Installation, lifecycle, health, recovery, and backup contracts are detailed in
[`linux-operations.md`](linux-operations.md). Native mount behavior is detailed
in [`linux-fuse.md`](../specs/linux-fuse.md). Matched range cost evidence is in
[`linux-range-cost-results.md`](linux-range-cost-results.md). Bounded sustained
operation, sampled resource growth and current limitations are recorded in
[`linux-operating-envelope.md`](linux-operating-envelope.md).
