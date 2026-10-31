# Linux operating envelope

**Status: passed for the two exact source/Amazon S3 combinations below.
Sampling gaps, phase-relative measurements, and result boundaries remain
explicit.**

This page records finite, bounded operating-envelope samples for exact `yy`
builds. They are not capacity ratings, service-level objectives, long-running
endurance guarantees or production sizing guides. Each measured combination is
recorded by separate artifact and qualification-script source, binary hash,
Ubuntu host identity, backend, dataset, concurrency and duration.

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

The workload deliberately uses whole-file Managed staging rather than claiming
delta upload. Current object-key inventory is sampled separately from local
staging and SQLite state. When versioning is enabled, retained object count and
stored bytes are sampled separately as well.

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
retained-version growth remain observable without that counter. Current-key
growth is not upload bytes or full history. A report without exact requests must
state that gap and must not convert TCP connections, GET calls made by the
verifier, or object count into application request count.

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

- pwrite and reader errors are zero;
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

Build or install the selected binary on Linux. The credential wrapper reads one
short-lived STS Credentials JSON document from stdin, removes inherited
credential and endpoint overrides, reads the measured artifact source from the
adjacent package/direct `BUILD-MANIFEST`, and invokes the measured binary:

```shell
aws sts assume-role ... --output json |
  YINYANG_QUALIFICATION_REGION=us-east-2 \
  YINYANG_OPERATING_ENVELOPE_BUCKET=replace-with-dedicated-bucket \
  YINYANG_OPERATING_ENVELOPE_YY="$HOME/.local/lib/yinyang/current/bin/yy" \
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

## Sparse sustained evidence (2026-11-01)

Run `o-s3-e5c6ead-042dfb4-1800s-r1` completed with runner exit `0` and report
status `pass`. The measured artifact source was
`e5c6ead739254261351d6bae46a16777f65035b3`, read from its direct package
`BUILD-MANIFEST`. The separate qualification-script source was
`042dfb4c7d5a156cdbced5517801f6a0296eb94a`. The measured ELF SHA-256 was
`bdfe412dc7351e478ed65bb74bf410c42614e468108f19fc0a27dc452ff9a923`.

The backend was a dedicated `us-east-2` Amazon S3 bucket in which versioning
had never been enabled. The dataset started at eight files of `8 MiB + 17 B`,
`67,109,000` logical bytes in total. Each phase computes its truncate limit
relative to that phase's starting files, so logical data can shrink across
phases; the observed phase-end totals were `58,851,464` and `50,593,928` bytes.

| phase | scheduled | phase span | mutations | reader attempts | validated | transition skips | restore bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| steady-state-one | 900.000s | 912.431s | 981 | 3,562 | 1,681 | 1,881 | 58,851,464 |
| steady-state-two | 900.000s | 920.022s | 818 | 3,571 | 1,670 | 1,901 | 50,593,928 |

`actual_seconds=900.000` in each phase is the workload scheduling interval.
The phase spans above include workload join, in-flight retry drain, and
background inventory join; they are not claims that every operation completed
at the 900-second boundary. The complete run wall time was `2,223.885439s`,
including setup, both phase spans, midpoint restore, final restore, inventory,
and health checks.

All `1,799` mutation-and-fsync units succeeded. Every file recorded successful
overwrite, append, truncate and zero-extension operations in both phases.
`pwrite_errors`, mutation errors, fsync errors, fsync retry attempts, retry
successes, uncertain files, active mutations and reader errors were all zero.
The zero retry count covers application fsync retries; it is not a
transport-level retry count.

Writer latency starts before opening the file and ends after mutation and
fsync. It excludes close and post-operation pacing. Across a phase, writer P50
was `3,470.125ms` then `4,409.759ms`; P95 was `4,512.298ms` then
`4,633.465ms`. These observations do not establish a stable latency trend, and
the second phase is not attributed to a particular cause.

Validated reader P50 was `327.988ms` then `323.019ms`; validated P95 was
`438.296ms` then `451.770ms`. Validated-read maxima were `674.583ms` and
`651.924ms`, while all reader-attempt maxima were `4,367.399ms` and
`4,947.448ms`. Attempt maxima must not be replaced by the smaller
validated-read maxima.

Actual overlap was observed rather than inferred from configured concurrency.
Both phases reached four concurrent writers, two concurrent readers and six
combined operations. `observed_overlap` includes reader attempts that skipped
before or during a mutation. `validated_read_overlap` includes only stable-epoch
reads that compared actual bytes with expected bytes:

| phase | attempt reader overlap | validated reader overlap |
| --- | ---: | ---: |
| steady-state-one | 515.469s | 461.214s |
| steady-state-two | 480.897s | 441.103s |

The midpoint and final independent restores each contained eight files and
matched the expected in-memory bytes exactly. Both clean unmounts returned
zero, remount readiness was restored, `staging.db` passed SQLite integrity
checks, and `doctor` passed. Status reported `pending=0`, `frozen=0`,
`conflicts=0`, `failed=0` and `ledger-errors=0`; no acknowledgment command was
needed or called.

The independent resource sampler produced `445` rows over the run with a
maximum observed gap of `5.003s`. Peak sampled RSS and high-water RSS were
`78,852,096` bytes, peak sampled staging allocation was `13,340,672` bytes, and
peak sampled open descriptors were `20`. These are sampled lower bounds rather
than continuous peak telemetry.

Current-key inventory moved from `67,487,943` bytes across `107` keys after
dataset creation to `184,325,734` bytes across `20,165` keys at the final
sample: `+116,837,791` bytes and `+20,058` keys. Ten inventory samples were
retained; completion timestamps had a maximum gap of `430.496s` because the
scans themselves ran in the background. This current-key growth is not upload
byte accounting, application request count, full version history, retained
history, or reclaimable bytes. Because the bucket was unversioned,
`retained_bytes` and `object_versions` remain unavailable.

Exact HTTPS request count remains unavailable; no compatible process-scoped
client counter was supplied, and current keys or verifier requests do not
substitute for that count.

Raw evidence:

- [accepted sparse operating-envelope JSON](benchmarks/linux-envelope-sparse-amazon-s3-2026-11-01-e5c6ead.json),
  321,090 bytes, SHA-256
  `ff05d618060a4de030aeaa85e9e9a159f16e5558fb098f20f6bcfbf1b7ac80e1`.

## Earlier same-length operating-envelope evidence

### Earlier source and artifact

The earlier integrated source is
`17361970d1722ea3d60c5381bbb5323d11818184`. Its default Ubuntu 24.04
x86_64 archive is
`yinyang-0.0.24-ubuntu-24.04-x86_64-17361970d172.tar.gz` with SHA-256
`ca339b309f0132b2a21bd685fd9cab75d5d5e982059344bba06e77f523dcffb4`.
The archive's `bin/yy` SHA-256 is
`a6724f23fd5b5120f14b17dd9290db2cd337ebe8f934758accc953317f3c154c`.

AWS repository gates passed at this source: `cargo x lint`, `cargo x check`,
`cargo x test`, `cargo x licenses`, and Rust 1.91.0 locked all-target check.
The accepted Core115 native proof at
`b630ef2` covers Managed FUSE 27 tests, operations 1, Managed Sync 12,
compatibility 8, recovery 4, and Direct 30 passed plus 8 filtered tests.
The subsequent A revision changed only the doctor remediation text in
`operations_cli.rs`; later harness/procedure and Python test-loader changes did
not alter native contracts. Those native workloads were not repeated at
`1736197`.

The installed-prefix upgrade
`o-pin-1736197-upgrade` passed against the immutable
`fd1338c2f924a90e9da752b9158e748d1d3acaf4` producer. It retained a
`74,192`-byte service WAL with SHA-256
`c5c186dba36d6b2fc866243f46541bf7c1f2a108f065164686c67b5cf122ad60`,
then converged target file bytes, Sync, service, receipt and doctor state.

### Earlier operating envelope

Run `o-pin-1736197-envelope-45m-r2` completed with runner exit `0` and report
status `pass`. It used the 24-file/256 KiB dataset, four range writers, two
range readers, the default intervals, and the dedicated
`yinyang-direct-q-35b90fdf-versioned` bucket in `us-east-2`.

| phase | planned | actual | writer ops | reader ops | pwrite/fsync/retry errors | uncertain |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| steady-state-one | 1350s | 1409.689s | 1345 | 3011 | 0 / 0 / 0 | none |
| steady-state-two | 1350s | 1728.775s | 1484 | 3469 | 0 / 0 / 0 | none |

The phase durations total `3138.464s`; the full report spans `3285.049s`.
Synchronous history collection and the final checks explain why neither is a
45-minute wall-clock claim. Resource sampling produced 243 rows rather than a
nominal five-second count of 540. The first phase retained 172 rows with a
maximum gap of `209.392s`; the second retained 71 rows with a maximum gap of
`441.46s`. These are sampled observations and lower bounds, not continuous
telemetry.

Both phases had empty `fatal` and `uncertain_files` lists.
`durability.status=known` with zero pwrite and fsync errors. The pre-doctor
status showed `pending=0 frozen=0 conflicts=0 failed=0 ledger-errors=0`; no
ledger acknowledgment command was needed or invoked. `doctor` passed, the
retained `staging.db` integrity check returned `ok`, both unmounts were clean,
and the 24-file/6,291,456-byte restores matched at the midpoint and final
check. The final paginated history contained 36,124 versions, zero delete
markers and 840,781,692 stored bytes across 37 pages. Peak sampled RSS/HWM was
35,586,048 bytes, peak staging allocation was 10,665,984 bytes, and peak open
descriptors was 22.

Exact Amazon S3 request count remains unavailable because the TLS path is
outside the plain-HTTP counting proxy and no compatible process-scoped counter
was supplied. The report does not infer requests from connections or verifier
calls.

Public raw evidence:

- [accepted operating-envelope JSON](benchmarks/linux-envelope-amazon-s3-2026-10-28-1736197.json),
  SHA-256
  `858a7dd40e3662b536bef7206ead861ee6261580ef18d2b2893fc01a4d089c0e`;
- [installed-upgrade JSON](benchmarks/linux-prefix-upgrade-2026-10-28-1736197.json),
  SHA-256
  `403bbcddbd6aac3cb5f6c1a5eae42d8b04b7720f7305b72a670ca7b56ff6b656`;
- [release build manifest](benchmarks/linux-release-manifest-2026-10-28-1736197.txt),
  SHA-256
  `d34e92efe67ac3e9b52d5574f5889c49e6c2b29e4fabe1b44b3fcda95343d37c`.

The archive is prepared for this qualification but is not published as a
release artifact.

### Earlier smoke and failure evidence

Earlier 90-second smoke runs are historical, not final proof. One exposed an
oracle defect where expected bytes advanced only after `fsync`; the harness now
advances the oracle after every successful `pwrite` segment and records fsync
uncertainty separately. Their raw files remain private development evidence and
are not used to support the final claims above.

The pre-fix 45-minute run at source `455842b` failed before midpoint when an
fsync retry deadline expired. The bounded `de86d1c` diagnostic classified the
observed terminal retry as CAS exhaustion rather than folded storage error, but
its runner exited `1` because doctor reported `ledger-errors=1`. These pre-fix
states motivated the accepted same-client serialization fix and guarded
acknowledgment procedure; they are not part of the final source, and the
diagnostic database was not acknowledged.
