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

GitHub Actions runs the FUSE and installed-CLI checks in the `linux-fuse` job
after starting isolated Silo. It runs backup and recovery acceptance in the
`s3` job after installing `sqlite3`. The broad `s3_` test filter explicitly
skips the two operations tests that require `/dev/fuse`, so the non-FUSE job
cannot report an accidental green or fail through a missing device.

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
| `python3 tests/linux_direct_real_s3.py` | task-owned versioned and never-versioned Amazon S3 buckets, temporary role, AWS CLI, `/dev/fuse` | read-only Direct pagination, reversible keys, version/ETag retention, conditional primitives and Linux FUSE lifecycle |
| `cargo x bench-linux-cost --workload read-only-range` | matched source identities, release binary, isolated S3, `/dev/fuse` | bounded FUSE range latency, requests, payload and staging |

The systemd lifecycle remains an explicit qualified-host command. Hosted CI
does not enable linger, create a boot service, or change FUSE configuration to
manufacture a user session. Run it on a disposable Linux host with an already
running user manager and retain the exact binary, host, storage service and
command evidence.

The Amazon S3 run qualifies only the bucket, region, endpoint and credential
mode actually recorded. A loopback Silo run does not imply Amazon S3 behavior,
and this matrix does not claim another S3-compatible provider.

The explicit real-Amazon-S3 Direct record is
[`linux-direct-amazon-s3.md`](linux-direct-amazon-s3.md). It qualifies only the
two listed buckets, default us-east-2 endpoint, temporary role and sampled
pagination/version behavior; it does not turn the loopback Silo suite into
Amazon S3 evidence.

## Matrix

The initial qualified combination is:

- Ubuntu 24.04 x86_64;
- one local systemd user manager on a single host;
- object publication or authenticated single-host loopback service publication;
- Managed Mount and finite ordinary-directory Managed Sync;
- read-only and conditional-writable Direct Mount plus finite Direct Sync;
- isolated MinIO-compatible Silo continuously, with Amazon S3 only when its
  explicit run is recorded.

The metadata service remains one authenticated loopback authority. This matrix
does not claim TLS, HA, network-safe service exposure, multi-host locking,
automatic Sync watching, or automatic garbage collection.

Deterministic Direct coverage uses isolated loopback Silo. Real Amazon S3
Direct qualification is a separate credentials-gated run and does not become a
matrix entry until its source, bucket, versioning, request and binary evidence
is recorded. Other S3-compatible services remain outside the matrix unless they
satisfy exact recursive LIST, conditional ETag mutation and the tested
version/range behavior.

Installation, lifecycle, health, recovery, and backup contracts are detailed in
[`linux-operations.md`](linux-operations.md). Native mount behavior is detailed
in [`linux-fuse.md`](../specs/linux-fuse.md). Matched range cost evidence is in
[`linux-range-cost-results.md`](linux-range-cost-results.md).
