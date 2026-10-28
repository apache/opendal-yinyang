# Linux operating envelope

**Status: harness prepared; final-target evidence pending.**

This page records one finite, bounded operating-envelope sample for an installed
`yy` build. It is not a capacity rating, service-level objective, long-running
endurance guarantee or production sizing guide. The measured combination is
recorded by exact source, binary hash, Ubuntu host identity, backend, dataset,
concurrency and duration.

## Selected workload

The default qualification workload is:

- Ubuntu 24.04 x86_64, one systemd user manager and one host;
- one installed Managed Mount with object publication on a dedicated,
  versioning-enabled Amazon S3 bucket;
- 24 files of 256 KiB, four concurrent range writers and two concurrent range
  readers;
- four-byte-page-aligned writers, each owning separate files, with `fsync` after
  every 4 KiB overwrite;
- a 45-minute target duration, split by one clean unmount/remount interval near
  the midpoint;
- independent latest-state restore and byte comparison after each phase.

The workload deliberately uses whole-file Managed staging rather than claiming
delta upload. Each acknowledged overwrite can retain a complete new object
version. Retained object count and stored bytes are sampled separately from
local staging and SQLite state.

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
it from connections.

Run the workload only in an otherwise excluded measurement window. Concurrent
latency benchmarks, native acceptance mounts and other S3 workloads can
contaminate p50/p95 values and request timing even when final bytes remain
correct.

## Current evidence

No final accepted implementation target has passed this operating-envelope
workload yet. Existing bounded loopback cost results, native restart/recovery
acceptance and the historical Amazon S3 operations smoke remain separate
evidence with narrower configurations.
