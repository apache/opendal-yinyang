# Linux cost benchmark

`cargo x bench-linux-cost` measures the current Linux product paths rather than
internal library loops. Every scenario invokes the release `yy` executable and
uses a real `/dev/fuse` mount or the Managed Sync CLI. One-shot
`yy publish`/`yy restore` scenarios use the object authority because that is the
only authority supported by those commands.

The default full and read-only matrices use isolated Silo (S3-compatible)
settings, a unique remote root, and a new staging directory. A loopback HTTP
counting proxy sits between YinYang and Silo. It parses HTTP/1 request framing
before counting request methods, and counts request and response bytes without
instrumenting production code. The proxy accepts connections without polling
and disables Nagle on both relay sockets so collector delay does not dominate
small range and proof requests. The real Amazon S3 repeated/random workload
instead uses the benchmark-only in-process telemetry described below.

Service publication uses a second transparent TCP proxy in front of the metadata
service. The current transport creates one loopback TCP connection for each RPC,
so connection count is also the RPC count. The proxy counts raw authenticated
frame bytes in both directions without decoding the private protocol. S3 and
service counters are separate in every record. The extra loopback hops are
visible in latency but not in byte or request counts; compare runs only with the
same proxies.

## Run

Run the command on Linux with `/dev/fuse`, `fusermount3`, and the isolated
storage environment used by `cargo x test-linux-fuse`:

```shell
cargo x bench-linux-cost \
  --profile early \
  --output target/linux-cost/early.jsonl
```

Run only one authority for a targeted rerun while preserving the selected
profile's scales:

```shell
cargo x bench-linux-cost \
  --profile full \
  --publication service \
  --output target/linux-cost/service.jsonl
```

Run the matched read-only range workload at 8 MiB and 32 MiB for both
authorities:

```shell
cargo x bench-linux-cost \
  --workload read-only-range \
  --output target/linux-cost/read-only-range.jsonl
```

This selector always writes read-only volume configurations. A baseline from an
older production commit must use the same collector, workload, file sizes,
range offsets, proxy chain, and read-only policy. Results from the earlier
writable FUSE fixtures are not an equivalent control because the writable path
also exercises publication-capable staging.

Run the matched writable incremental workload for both authorities:

```shell
cargo x bench-linux-cost \
  --workload incremental \
  --incremental-expect baseline \
  --output target/linux-cost/incremental-baseline.jsonl
```

Use the same command with `--incremental-expect required` on an implementation
that must avoid full-file fallback for same-length updates. Required mode fails
if writable open materializes more than half of the 32 MiB file, or if any
same-length overwrite plus fsync reads or writes more than half of the file
through S3. Both proxies stay outside the measured process, so service RPC
aggregation cannot hide storage GET or PUT work.

Run the matched length-changing workload for both authorities:

```shell
cargo x bench-linux-cost \
  --workload length-changing \
  --incremental-expect baseline \
  --output target/linux-cost/length-changing-baseline.jsonl
```

Use the same command with `--incremental-expect required` on an implementation
that must avoid complete-source fallback for length-changing writes. The
workload uses one 32 MiB file, a shared read-only descriptor and an active
append descriptor, two 4 KiB appends, two 68 KiB appends that each cross an
existing 64 KiB verification-unit boundary, two 4 KiB truncates, two 4 KiB zero
extensions, two shrink-then-extend rounds, unchanged fsync, and remount plus
tail reads. Operation scenarios keep two latency samples; the read-only and
writable descriptor opens are separate one-sample scenarios and are never
merged into one p50/p95 distribution. Two samples are descriptive only; the
summary p50/p95 is not a stable performance claim. Complete remote bytes are
verified outside each measured interval. Required mode fails if
writable open, preparation, or publication transfers at least half of the file
through S3, or if allocated mount staging is at least half of the file. JSONL
records retain `useful_bytes` per sample, raw latency samples, GET/PUT request
and body counts, service RPC counts, staging series, retained storage series,
and the negative unchanged-fsync observation.

The `early` profile runs the small object-authority Sync, FUSE, concurrent-writer,
and one-shot transfer scenarios. The `full` profile adds the service authority,
the medium scale, and the explicit service-authority one-shot boundary. The
`--publication` filter defaults to `all`; `object` and `service` select only
that authority. The command writes JSONL evidence to `--output` and a Markdown
summary to the same path with an `.md` extension. Every record and summary
reports the workload, volume mode, publication filter, and the
measured/unsupported record counts so a filtered run cannot be mistaken for the
complete matrix.

The summary marks combinations that the product does not support as
`unsupported` with dashes for measurement fields. This prevents a not-run
service-authority one-shot transfer from looking like a measured zero-cost
operation.

Use an empty output path for each run. Keep `--release` output on one commit and
host while comparing implementations; the tool records the source commit, dirty
state, binary path, and binary BLAKE3 digest in every record.

## Workloads

The small scale uses 32 files of 1 KiB in four directories. The medium scale uses
128 files of 1 KiB in eight directories. FUSE large-file scenarios use 8 MiB and
32 MiB files and read 4 KiB ranges. Concurrent-writer scenarios use two writers
at the small scale and four writers at the medium scale, each writing a distinct
256 KiB file.

The matrix covers:

- Managed Sync initial publication, repeated no-change passes, a single-file
  edit, and rename;
- FUSE cold open plus a small range read, warm range reads on the same
  descriptor, four sparse updates, a remount/restart, and concurrent writers;
- one-shot object-authority `publish` and `restore`.

The `read-only-range` workload covers both authorities at 8 MiB and 32 MiB:
a cold open plus one 4 KiB read, five warm reads on the same descriptor, a
restart, and a cold open plus one 8 KiB read spanning a 64 KiB verification-unit
boundary. It records allocated mount staging while the read descriptor remains
open, after the reads, and after restart. These samples expose local
materialization even if a later release reclaims a clean staged file.
Warm here means repeated reads through the same `FileVersion`; it does not imply
local staging or a persistent content cache. The measured requests and bytes
must determine whether those reads still fetch remote units or proofs.
The bounded 2026-10-27 matched result is recorded in
[`linux-range-cost-results.md`](linux-range-cost-results.md).

The `repeated-random` workload measures one Managed object authority through a
read-only FUSE mount against a direct Amazon S3 HTTPS endpoint. It adds
same-offset repetition, an 8 MiB hot working set, over-capacity random access,
restart, forced eviction, and replacement with an old descriptor. The optional
`benchmark-telemetry` feature installs the released OpenDAL MetricsLayer in the
measured `yy` process, so request counts and HTTP body bytes are collected
without a proxy or TLS termination. Its exact source/binary identities,
per-batch accounting, memory tradeoff, negative cases, and raw evidence are in
[`linux-verified-range-reuse.md`](linux-verified-range-reuse.md).

The `incremental` workload uses one 32 MiB writable FUSE file and both object
and service publication. It measures writable open separately from three
same-length overwrites: one inside a verification unit, one crossing a 64 KiB
unit boundary, and one at EOF. Each overwrite is followed by fsync and an
independent exact-byte check. It also records unchanged fsync, remount plus a
read spanning the final updates, local staging, retained storage, and negative
tradeoffs in the raw samples. Length-changing append or truncate behavior is
covered by functional FUSE acceptance and is not included in the same-length
fallback assertion.

The same workload also runs a matched serial-versus-four-writer write latency
probe. Each case uses three batches of four pre-created 8 MiB files. Every file
has the same deterministic initial bytes and receives one 4 KiB overwrite at the
same relative offset. The single writer completes open, mutation, fsync, and
close for each file before starting the next. Four long-lived workers cross one
start barrier, then each completes the same lifecycle for three files. Each
end-to-end latency sample spans the continuous open, mutation, and fsync for one
file. The record also retains separate open, mutation, and fsync duration
arrays, but their sum is not substituted for the observed worker interval or
the batch wall time. Complete remote bytes are checked after all workers finish,
outside every measured interval.

The counting proxies record request intervals independently of the configured
writer count. The report exposes application-operation overlap, S3 request
overlap, and metadata-service RPC overlap as separate observations. Four active
application writers with maximum remote concurrency one is a valid serialized
authority result. Concurrent network values remain whole-batch totals; the
collector does not subtract global proxy counters around one writer and assign
other writers' traffic to that sample.

The accepted matched result and raw 16-row evidence are in
[`linux-incremental-write-results.md`](linux-incremental-write-results.md).

The current writable workloads record schema `yinyang.linux-cost.v7`, which
adds application and remote activity summaries to the v6 `useful_bytes`
collector shape. The complete-source fallback in a pre-optimization baseline is
expected to read and publish most of the file for each length change, while
`required` mode rejects that behavior.

The accepted matched result, binary identities, per-operation traffic, staging
tradeoff, and negative observations are in
[`linux-length-changing-write-results.md`](linux-length-changing-write-results.md).

A Managed file open pins authenticated metadata without reading payload bytes.
The first read of a missing verification unit authenticates and fetches that
range from the pinned version. Writable staging records exact changed segments
over that pinned base; publication advances the complete logical content
descriptor without downloading unaffected bytes.

`fuse_mount_ready` measures from before spawning `yy mount` until
`/proc/mounts` reports `fuse.yinyang`, including daemon process I/O and proxy
traffic. `fuse_restart_mount_ready` measures the same interval for the
replacement mount. `fuse_restart_cold_open_range_read` starts only after that
replacement mount is ready, so remount startup is not hidden in the restart-read
latency.

## Evidence

Each JSONL record includes:

- source commit, dirty state, release binary path, binary BLAKE3 digest, host,
  CPU/memory identity, and Rust version;
- dataset file count, directory count, logical bytes, file size, and range size;
- completion semantics for the frontend;
- workload and read-only/read-write volume mode;
- every latency sample and p50/p95/min/max;
- sampled process `rchar`, `wchar`, syscall counts, and block-layer read/write
  bytes from `/proc/<pid>/io`;
- S3 requests by HTTP method, total request bytes, and total response bytes;
- S3 GET response-body bytes and PUT request-body bytes, excluding HTTP headers;
- service TCP connections and raw framed bytes in both directions;
- batch totals and per-sample network values, where per-sample divides only a
  sequential batch and explicitly notes that concurrent samples overlap;
- application-operation overlap plus S3 request and metadata-service RPC
  interval summaries, including maximum observed concurrency and in-flight
  work at the end of each phase;
- allocated local payload, Sync state, mount staging, SQLite database plus
  `-wal`/`-shm`, and persistent S3 object bytes and counts;
- assertions that completed successfully and the failure count (zero for a
  completed scenario).

Raw network counters aggregate over all iterations in a scenario. `iterations`
is the actual latency sample count. The summary shows both `total/sample` values
so a five-run no-op batch is not presented as one run. A concurrent writer batch
shares one network interval, so its per-sample network values are descriptive
shares, not independent operation costs.

`/proc/<pid>/io` is polled while the process is alive, so terminal increments can
be missed and every value is a lower bound. Character counts also include socket
I/O. Block read/write bytes identify actual local block-layer work, while the
dataset and persistent-space fields provide the storage-accounting context. Do
not report one of these fields as exact local-only scan or hash work when the
process also read or wrote the network.

Sync completion is the successful return of one finite `yy sync run` followed by
an independently observed `status --json` with `summary.in_sync=true`; that
status observation is outside the reported run latency. FUSE sparse-update and
concurrent-writer completion is the successful `fsync` of every descriptor. The
benchmark verifies the resulting remote bytes after the measured interval. These
are not interchangeable with a File Provider local save, which is outside this
Linux benchmark.

## Limits

The benchmark does not claim production capacity, cross-host performance, or a
performance regression threshold. Default matrices measure one isolated
loopback Silo on the recorded host; the repeated/random report records its
separate real Amazon S3 region, host and sampling limits. These measurements do
not replace the functional Linux acceptance suites. Do not commit generated
datasets or large build artifacts; retain only bounded evidence and reusable
tooling in the repository.
