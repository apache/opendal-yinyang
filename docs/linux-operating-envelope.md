# Linux operating envelope

**Status: harness prepared; final-target evidence pending.**

This page records one finite, bounded operating-envelope sample for an installed
`yy` build. It is not a capacity rating, service-level objective, long-running
endurance guarantee or production sizing guide. The measured combination is
recorded by exact source, binary hash, Ubuntu host identity, backend, dataset,
concurrency and duration.

## Selected workload

The default qualification workload is:

- Ubuntu 24.04 x86_64 on one host; the installed systemd lifecycle is covered
  separately and is not a prerequisite for this harness;
- one built or installed Managed Mount with object publication on a dedicated,
  versioning-enabled Amazon S3 bucket;
- 24 files of 256 KiB, four concurrent range writers and two concurrent range
  readers;
- 4 KiB page-aligned writers, each owning separate files, with `fsync` after
  every 4 KiB overwrite;
- a 45-minute target duration, split by one clean unmount/remount interval near
  the midpoint;
- independent latest-state restore and byte comparison after each phase.

The workload deliberately uses whole-file Managed staging rather than claiming
delta upload. Each acknowledged overwrite can retain a complete new object
version. Retained object count and stored bytes are sampled separately from
local staging and SQLite state.

The harness does not delete the remote prefix after a run. Its owner must
remove the isolated prefix after preserving the JSON evidence; the verifier
does not silently clean up retained-history evidence.

## Measurements

The raw JSON evidence contains:

- Mount process RSS, high-water RSS, thread count and open descriptor count,
  sampled from `/proc/PID/status` and `/proc/PID/fd`;
- process `rchar`, `wchar`, read/write byte and syscall counters sampled from
  `/proc/PID/io`; character I/O includes sockets and is not an exact network
  accounting source;
- allocated bytes under Mount staging and the local state parent, including
  SQLite WAL and shared-memory files;
- available bytes on the state filesystem;
- remote version and delete-marker counts plus stored bytes from paginated
  version-aware S3 listing;
- exact operation counts, errors, p50 and p95 for fsync writes and page reads;
- unmount exit, remount readiness, `doctor` result, SQLite integrity and final
  latest-state bytes compared with the expected in-memory dataset.

File counts, dimensions, concurrency, sampling period and duration are bounded
inputs, not extrapolated capacity. A run that exceeds the bounded interval or
dataset is separate evidence.

## Request accounting

The real Amazon S3 path uses TLS outside the repository's plain-HTTP counting
proxy, so transport-level connection counts are not S3 request counts. The
qualification report leaves request count unavailable unless a safe,
process-scoped client counter from the performance harness is supplied for the
same source binary and time window. AWS CloudWatch request metrics are not
enabled solely to manufacture this number because enabling request metrics and
retrieving them is a paid external resource change.

`/proc/PID/io`, local staging growth and remote version growth remain observable
without that counter. A report without exact requests must state that gap and
must not convert TCP connections, GET calls made by the verifier, or object
count into application request count.

## Fault boundaries

This sustained sample performs one deliberate process restart through clean
unmount/remount and verifies the acknowledged dataset after restart. It does not
repeat every outage, credential-failure and disk-exhaustion case already covered
by native recovery and operations acceptance. Those cases remain separate
evidence and are linked rather than relabelled as soak data.

The workload does not establish:

- automatic garbage collection or bounded retained history;
- a production throughput, latency, connection or concurrency limit;
- multi-host coordination, HA, online backup or network-safe metadata service
  exposure;
- behavior after the tested duration or outside the exact backend and host;
- stable release, cryptographic signing or support for arbitrary upgrades.

## Reproduce

Build or install the selected binary on Linux. The credential wrapper reads one
short-lived STS Credentials JSON document from stdin, removes inherited
credential and endpoint overrides, and invokes the measured binary:

```shell
aws sts assume-role ... --output json |
  YINYANG_QUALIFICATION_REGION=us-east-2 \
  YINYANG_OPERATING_ENVELOPE_BUCKET=replace-with-dedicated-versioned-bucket \
  YINYANG_OPERATING_ENVELOPE_YY="$HOME/.local/lib/yinyang/current/bin/yy" \
  YINYANG_OPERATING_ENVELOPE_OUTPUT=/var/tmp/yinyang-envelope.json \
  YINYANG_OPERATING_ENVELOPE_DURATION=2700 \
  AWS_CLI_BIN=/absolute/path/to/aws \
  python3 tests/linux_operating_envelope_real_s3.py
```

The wrapper never prints, writes or places credential values in command
arguments. `AWS_CLI_BIN` is required when the official AWS CLI is intentionally
installed outside `PATH`. The bucket must already have versioning enabled. The
harness performs read-only versioning and listing preflight checks and refuses
to begin if remote retained-history accounting is unavailable.

When a compatible process-scoped request counter exists for the same source and
binary, set `YINYANG_OPERATING_ENVELOPE_REQUEST_ACCOUNTING` to its JSON path.
Otherwise the report records request count as unavailable rather than inferring
it from connections. The accepted document must use
`yinyang.linux-request-accounting.v1`, include the exact `source_sha` and
`binary_sha256`, and expose the counter windows and their accounting scope.

Run the workload only in an otherwise excluded measurement window. Concurrent
latency benchmarks, native acceptance mounts and other S3 workloads can
contaminate p50/p95 values and request timing even when final bytes remain
correct.

## Current evidence

The operating-envelope harness has passed a 90-second real Amazon S3 smoke at
source `79506c0684c47b37bc93364eaa7192d65a770ede` with binary SHA-256
`b4985f099f1aff9447a2990f2ec3bdc8945237732eb8985a576f7666147cd979`.
It used the dedicated `yinyang-direct-q-35b90fdf-versioned` bucket in
`us-east-2` and the 24-file/256 KiB default dataset. The two workload phases
ran for 48.156 and 48.552 seconds; final version-list verification added time,
so this is not presented as a 90-second wall-clock result.

The smoke reached the following bounded observations:

- peak sampled Mount RSS and high-water RSS: 27,070,464 bytes;
- peak Mount staging allocation: 10,608,640 bytes;
- peak observed open descriptors: 21;
- two clean unmounts and one successful remount;
- final latest-state restore: 24 files, 6,291,456 bytes;
- final S3 history: 3,558 object versions, 0 delete markers, 41,388,760 stored
  bytes across 4 version-list pages;
- 48 successful fsync-write operations in each phase, with no writer/reader
  errors;
- `doctor` passed and the retained SQLite `staging.db` integrity check reported
  `ok`.

An earlier 90-second run at source `7fd4d87602f3aa98b53dca31eeca9bf6d8ce2f3e`
reported a same-length mismatch for `file-0015.bin` during the first-phase
restore. The harness was strengthened to compare the mounted view before
unmount and to record both byte hashes; the repeat at `79506c0` passed. The
cause of the original mismatch is not established, so neither run is treated
as a stable capacity result. The final integrated soak must detect,
reproduce or explain this residual risk before claiming a settled operating
envelope.

Request count is unavailable for this smoke because no compatible
process-scoped counter was supplied; the report keeps that field explicit
rather than inferring it from local I/O. The raw JSON is retained outside the
repository at
`/Users/xuanwo/Code/experiments/yinyang/linux-next-2026-10-28/O-envelope-smoke-79506c0.json`.

A separate installed-prefix run at the same source upgraded the
`fd1338c` producer to the target release binary, preserved a 74,192-byte
service WAL before target startup, and passed target file, Sync, service,
receipt and `doctor` checks. Its digest is recorded in
`/Users/xuanwo/Code/experiments/yinyang/linux-next-2026-10-28/O-upgrade-79506c0.json`.
That run is preparatory evidence for the operations harness; it does not
qualify the final integrated implementation.

The local Ubuntu 24.04 archive built from the same source has SHA-256
`4368831f2bd3b9729caaaed511312749e9f98100a3d22a733a339222a9da159c`; the
archive's binary hash matches the smoke target. No final 45-minute
operating-envelope run has been made against the integrated
P/W/M/S candidate yet.
