# Linux cost benchmark

`cargo x bench-linux-cost` measures the current Linux product paths rather than
internal library loops. Every scenario invokes the release `yy` executable and
uses a real `/dev/fuse` mount or the Managed Sync CLI. One-shot
`yy publish`/`yy restore` scenarios use the object authority because that is the
only authority supported by those commands.

The benchmark uses isolated Silo (S3-compatible) settings, a unique remote root,
and a new staging directory. A loopback HTTP counting proxy sits between YinYang
and Silo. It counts request methods, request bytes, and response bytes without
instrumenting production code. Metadata-service traffic is direct; the byte
counts cover storage traffic only.

## Run

Run the command on Linux with `/dev/fuse`, `fusermount3`, and the isolated
storage environment used by `cargo x test-linux-fuse`:

```shell
cargo x bench-linux-cost \
  --profile early \
  --output target/linux-cost/early.jsonl
```

The `early` profile runs the small object-authority Sync, FUSE, concurrent-writer,
and one-shot transfer scenarios. The `full` profile adds the service authority,
the medium scale, and the explicit service-authority one-shot boundary. The
command writes JSONL evidence to `--output` and a Markdown summary to the same
path with an `.md` extension.

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

The first FUSE open materializes the complete file, so a cold 4 KiB read is
expected to transfer much more than 4 KiB. A sparse update starts from the
materialized staging file and publishes a new complete content version. These
are current behavior boundaries, not benchmark assumptions.

## Evidence

Each JSONL record includes:

- source commit, dirty state, release binary path, binary BLAKE3 digest, host,
  CPU/memory identity, and Rust version;
- dataset file count, directory count, logical bytes, file size, and range size;
- completion semantics for the frontend;
- every latency sample and p50/p95/min/max;
- process `rchar`, `wchar`, syscall counts, and block-layer read/write bytes from
  `/proc/<pid>/io`;
- storage requests by HTTP method, total request bytes, and total response bytes;
- local payload, Sync state, mount staging, metadata database, and persistent S3
  object bytes and object counts;
- assertions that completed successfully and the failure count (zero for a
  completed scenario).

`/proc/<pid>/io` character counts include socket I/O as well as file I/O. Block
read/write bytes identify actual local block-layer work, while the dataset and
persistent-space fields provide the storage-accounting context. Do not report
one of these fields as local-only file I/O when the process also read or wrote
the network.

Sync completion is the successful return of one finite `yy sync run` followed by
an independently observed `status --json` with `summary.in_sync=true`. FUSE
sparse-update and concurrent-writer completion is the successful `fsync` of
every descriptor; the benchmark verifies the resulting remote namespace. These
are not interchangeable with a File Provider local save, which is outside this
Linux benchmark.

## Limits

The benchmark does not claim production capacity, cross-host performance, or a
performance regression threshold. It measures one isolated loopback Silo on the
recorded host. It does not replace the functional Linux acceptance suites. Do
not commit generated datasets or large build artifacts; retain only bounded
evidence and reusable tooling in the repository.
