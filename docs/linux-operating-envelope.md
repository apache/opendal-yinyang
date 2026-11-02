# Linux operating envelope

**Status: two exact source/Amazon S3 combinations passed the bounded workload
described below. The result records are retained outside the repository.**

This page documents how to run the finite, bounded operating-envelope workload.
Its results are not capacity ratings, service-level objectives, long-running
endurance guarantees or production sizing guides.

## Selected workload

The v2 qualification harness defaults to:

- Ubuntu 24.04 x86_64 on one host; the installed systemd lifecycle is covered
  separately and is not a prerequisite for this harness;
- one built or directly downloaded Managed Mount with object publication on a
  dedicated Amazon S3 bucket; the bucket versioning state is recorded and
  retained-version inventory is unavailable when versioning is disabled;
- eight files of `8 MiB + 17 B`, four writers and two readers; each writer owns
  separate files;
- per-file mixed cycles containing 4 KiB overwrite, 4 KiB append, bounded
  truncate, and 4 KiB zero-extension mutations, with `fsync` after every
  mutation;
- a 30-minute target duration split into two 900-second phases by one clean
  unmount/remount near the midpoint;
- independent latest-state restore and byte comparison at the midpoint and
  final state.

The workload exercises sparse Managed staging and the existing bounded
prepared-update path for length changes. It does not infer actual upload or
delta bytes from current-key inventory. Current object keys are sampled
separately from local staging and SQLite state. When backend versioning is
enabled, backend version counts and stored bytes are sampled separately as well.

The harness does not delete the remote prefix after a run. Its owner must
remove the isolated prefix after preserving the JSON evidence; the verifier does
not silently clean up retained-history evidence.

## Measurements

The raw JSON evidence contains:

- Mount process RSS, high-water RSS, thread count and open descriptor count,
  sampled from `/proc/PID/status` and `/proc/PID/fd` by an independent periodic
  sampler;
- process `rchar`, `wchar`, read/write byte and syscall counters sampled from
  `/proc/PID/io`; character I/O includes sockets and is not an exact network
  accounting source;
- allocated bytes under Mount staging and the local state parent, including
  SQLite WAL and shared-memory files;
- available bytes on the state filesystem;
- paginated current-key counts and bytes from `ListObjectsV2`; when versioning
  is enabled, paginated `ListObjectVersions` totals are added separately;
- operation counts, bytes, errors, and latency summaries for overwrite, append,
  truncate and zero-extension mutations;
- reader attempts, stable-epoch byte validations, concurrent-transition skips,
  short reads and validation mismatches;
- actual writer/reader interval overlap for all reader attempts and separately
  for stable-epoch validated reads;
- unmount exit, remount readiness, `doctor` result, SQLite integrity and final
  latest-state bytes compared with the expected in-memory dataset.

The expected-byte oracle advances after every successful mutation segment, not
only after the following `fsync`. Partial writes therefore remain represented
in the local and remote byte assertions. Mutation and `fsync` failures are
counted separately; a file remains durability-unknown until a later successful
fsync covers its dirty bytes. After a failed `fsync`, the workload retries the
same descriptor with bounded backoff for `--fsync-retry-seconds` (default 30
seconds) without issuing a new mutation. The retry counts and first operation
errors are retained in the phase report. The run fails when the retry deadline
expires; three consecutive mutation failures also abort the phase. Otherwise
any unresolved uncertainty makes the run fail after the byte assertions are
evaluated and recorded.

A reader first observes the file epoch and mutation state, performs one random
4 KiB `pread` without a file lock covering that I/O, then compares the bytes
only if the epoch is unchanged and no mutation is active. A concurrent
transition is recorded as a skip rather than corruption. Stable-epoch short
reads and content mismatches fail the run.

After the workload joins, the phase report retains a per-file length and
SHA-256 manifest derived from the in-memory expected buffers. It is labeled
`pwrite-acknowledged expected bytes`; it is not a claim of remote equality
while fsync or publication is still uncertain.

File counts, dimensions, concurrency, mutation bounds, sampling period and
duration are bounded inputs, not extrapolated capacity. A run that exceeds the
bounded interval or dataset is separate evidence.

## Request accounting

The real Amazon S3 path uses TLS outside the repository's plain-HTTP counting
proxy, so transport-level connection counts are not S3 request counts. The
qualification report leaves request count unavailable unless a safe,
process-scoped client counter from the performance harness is supplied for the
same source binary and time window. AWS CloudWatch request metrics are not
enabled solely to manufacture this number because enabling request metrics and
retrieving them is a paid external resource change.

`/proc/PID/io`, local staging growth, current-key growth and, when available,
backend retained-version growth remain observable without that counter.
Current-key growth is not upload bytes or complete retained-history accounting;
retained YinYang snapshots and shared packs can remain among current keys. A
report without exact requests must state that gap and must not convert TCP
connections, GET calls made by the verifier, or object count into application
request count.

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

## Operational acknowledgment

A recovered fsync can still leave a durable error-ledger entry. Doctor remains
`action_required` until an operator explicitly acknowledges the reviewed
entries with `yy --volume CONFIG file acknowledge-errors THROUGH`. This removes
ledger rows through the observed sequence; it does not resolve pending data,
frozen requests, conflicts, failed handles, or uncertain durability.

The harness first stops and releases the mount, verifies independently restored
bytes, samples remote inventory, and checks SQLite integrity. It then captures
the complete raw `yy --volume CONFIG status` output, retry counters and parsed
handle/ledger counts. It will not acknowledge any entry unless all of the
following hold:

- mutation errors (including pwrite) and reader errors are zero;
- each phase has `fsync_retry_attempts == fsync_errors`,
  `fsync_retry_successes <= fsync_retry_attempts`, and retry successes are
  positive exactly when fsync errors are positive; no file remains uncertain;
- fatal lists are empty and both unmounts report a clean release;
- pending, frozen, conflict and failed-handle counts are zero;
- every retained ledger row is persisted, has a positive sequence, and is
  present in the complete ordered status capture.

When retained rows exist, `completion.acknowledgment` records the exact
acknowledgment command, the selected `through` sequence, its result, and
complete post-acknowledgment status. The post-status pending, frozen, conflict,
failed-handle and ledger counts must all be zero before the harness accepts
doctor. Doctor must subsequently return `status=pass`; a nonzero exit or
non-pass status still fails the run and retains the raw error.

Invoking this harness authorizes its guarded acknowledgment only on the
isolated, fresh test state that the harness created. Raw status and error
evidence remains in the report. Existing or other non-test volumes require
explicit operator review and are outside this workflow. Acknowledgment is not
an error suppressor: failed or diagnostic staging remains unchanged and must
never be acknowledged merely to make a later health check green.

## Reproduce

Use the exact binary and its adjacent package/direct `BUILD-MANIFEST` on Linux.
The credential wrapper reads one short-lived STS Credentials JSON document from
stdin, removes inherited credential and endpoint overrides, reads the measured
artifact source from that manifest, and invokes the measured binary:

```shell
aws sts assume-role ... --output json |
  YINYANG_QUALIFICATION_REGION=us-east-2 \
  YINYANG_OPERATING_ENVELOPE_BUCKET=replace-with-dedicated-bucket \
  YINYANG_OPERATING_ENVELOPE_YY="$PWD/yy" \
  YINYANG_OPERATING_ENVELOPE_OUTPUT=/var/tmp/yinyang-envelope.json \
  YINYANG_OPERATING_ENVELOPE_DURATION=1800 \
  YINYANG_OPERATING_ENVELOPE_FILES=8 \
  YINYANG_OPERATING_ENVELOPE_FILE_BYTES=8388625 \
  YINYANG_OPERATING_ENVELOPE_WRITERS=4 \
  YINYANG_OPERATING_ENVELOPE_READERS=2 \
  AWS_CLI_BIN=/absolute/path/to/aws \
  python3 tests/linux_operating_envelope_real_s3.py
```

The wrapper never prints, writes or places credential values in command
arguments. `AWS_CLI_BIN` is required when the official AWS CLI is intentionally
installed outside `PATH`. The bucket may be versioned or unversioned. The
harness records both the measured artifact source and the harness checkout
source separately. It performs read-only versioning and current-key listing
preflight checks. Retained version inventory is explicitly unavailable when
versioning is disabled.

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

## Qualified bounds

On one four-vCPU Ubuntu 24.04 host against a dedicated unversioned `us-east-2`
Amazon S3 bucket, the v2 workload completed two 900-second phases with eight
files of `8 MiB + 17 B`, four writers, and two readers. All 1,799
mutation-and-fsync units succeeded, both independent restores matched, both
unmounts were clean, SQLite integrity and doctor passed, and the observed
current-key growth was not reclaimable-space accounting. This qualified bound
is recorded in the pull request that introduced it (#133).

The earlier same-length workload completed two 1,350-second phases with 24
files of 256 KiB, four writers, two readers, and a dedicated versioned
`us-east-2` bucket. Both phases had zero pwrite, fsync, retry, and uncertainty
errors; the midpoint and final restores matched. This qualified bound is
recorded in the pull request that introduced it (#116).

Both runs observed finite source, binary, backend, host, duration, and
concurrency combinations only. Sampled resource gaps, current-key growth,
request-accounting limits, and the absence of long-running or production
capacity claims remain part of those records.
