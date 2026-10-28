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

The first FUSE open materializes the complete file, so a cold 4 KiB read is
expected to transfer much more than 4 KiB. A sparse update starts from the
materialized staging file and publishes a new complete content version. These
are current behavior boundaries, not benchmark assumptions.

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
- service TCP connections and raw framed bytes in both directions;
- batch totals and per-sample network values, where per-sample divides only a
  sequential batch and explicitly notes that concurrent samples overlap;
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
