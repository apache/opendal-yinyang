# Linux Managed write lifecycle results

Date: 2026-11-01, Asia/Shanghai.

## Result

This baseline measures a continuous user-visible Managed write lifecycle:
writable open, one 4 KiB overwrite, and fsync, followed by close before the
worker starts its next file. The test covers one long-lived writer and four
long-lived writers over the same twelve 8 MiB files through object and service
publication.

Fsync dominates the lifecycle. Writable open is roughly an eighth of the total,
and the local SQLite-backed mutation is the smallest contribution. Four workers
reduce whole-batch wall time while increasing each file's end-to-end latency.
The proxies observed four simultaneous S3 requests and up to six simultaneous
metadata-service RPCs, so the concurrency result is measured rather than
inferred from the configured writer count.

The values are one bounded loopback-Silo run. Twelve file samples were collected
in three correlated rounds; they are not twelve independent production
distributions and do not establish a stable p95 or capacity threshold.

## Tested identity

- Source: `783d332142e84bcc6125e432ad5fcddf8cdef93e`
- `tests/linux_cost.rs` blob:
  `166a07400a61c74680b6865626532543bbb11916`
- `tests/linux_incremental_cost_capture.sh` blob:
  `060503b29a51f916ecad785a81510134a1cefc16`
- Release ELF BLAKE3:
  `88975b7bb548f154e1e0459e8fe62fbe94eed8e4b247e0b8da312bde7cd593f8`
- Release ELF SHA-256:
  `7c7669c676b2275ceec1a3c15a1b390293797265ee4f400466d2f3c3641ae0c9`
- Host: Linux `lance-fuse-demo`, Ubuntu 24.04 x86_64, four vCPUs, 16 GiB RAM
- Rust: `1.99.1`
- Storage: isolated loopback Silo over HTTP
- Runner: `mwlat-lifecycle-783d332-r1`
- Rust test and capture wrapper: exit `0`

## Workload

The dataset is twelve 8 MiB files with distinct deterministic initial content.
Each file receives the same 4 KiB replacement at the same aligned relative
offset. The serial case uses one worker to complete open, overwrite, fsync, and
close for all twelve files. The concurrent case uses four long-lived workers
behind one start barrier; each worker completes the same lifecycle for three
files. There is no phase barrier between open, mutation, and fsync.

The end-to-end sample for one file starts immediately before open and ends when
that file's fsync returns. The batch wall starts at the earliest worker start
and ends at the latest worker finish. Network settling, storage listing, and
complete remote-byte verification happen after the batch wall stops. Network
counters are therefore whole-batch totals and are not attributed to one writer's
phase.

## Lifecycle latency

| Authority | Writers | Samples | p50 ms | p95 ms | Batch wall ms | S3 GET/HEAD/PUT | Service RPCs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| object | 1 | 12 | 90.639 | 96.762 | 1,098.388 | 452/72/84 | 0 |
| object | 4 | 12 | 193.854 | 283.066 | 665.116 | 458/72/84 | 0 |
| service | 1 | 12 | 69.968 | 73.973 | 845.801 | 132/12/12 | 161 |
| service | 4 | 12 | 103.649 | 164.944 | 367.399 | 132/12/12 | 161 |

The phase p50 diagnostics from the same records are:

| Authority | Writers | Open ms | Mutation ms | Fsync ms |
| --- | ---: | ---: | ---: | ---: |
| object | 1 | 13.428 | 4.390 | 73.394 |
| object | 4 | 14.308 | 4.805 | 173.556 |
| service | 1 | 12.746 | 3.267 | 54.045 |
| service | 4 | 23.074 | 8.181 | 75.528 |

The phase arrays explain where time is spent. They are not added to reconstruct
the end-to-end sample or batch wall.

## Concurrency

| Authority | Max S3 requests | S3 overlap ms | Max service RPCs | RPC overlap ms | Batch wall change |
| --- | ---: | ---: | ---: | ---: | ---: |
| object | 4 | 323.641 | 0 | 0 | -39.4% |
| service | 4 | 70.170 | 6 | 410.599 | -56.6% |

Per-file p50 latency increased from 90.639 to 193.854 ms for object publication
and from 69.968 to 103.649 ms for service publication. The concurrent case
therefore improves aggregate wall time while increasing contention observed by
each writer.

Object publication reaches content and metadata-request concurrency four but
individual fsyncs wait materially longer. Service publication overlaps more RPC
work and has the larger whole-batch improvement.

## Attribution boundary

Fsync owns most of the lifecycle in both authorities. Open performs metadata
observation and staging setup without reading file payload. Mutation records
the exact dirty segment in SQLite with `synchronous=FULL`. Fsync then performs
bounded content preparation, immutable pack publication and readback
verification, metadata observation, conditional commit, and post-commit base
refresh.

The next attribution target is the repeated exact-base metadata observation in
`FileHandle::publish`: preparation uses the handle's pinned `FileVersion`, then
planning calls `observe_revision` for the same revision again. Retaining the
open-time `Snapshot` for that exact revision can remove one observation without
changing the original conditions or frozen transaction identity. This report
does not claim an optimization result.

## Evidence

- [Raw JSONL](benchmarks/linux-cost-write-latency-2026-11-01-baseline-783d332.jsonl):
  24 measured records, SHA-256
  `4ca7aac4abf2c970f4ecd0f4d7a430b55c7df45b7de0240bdae1daddd6705f6c`

The raw records retain source and binary identity, exact datasets, every
end-to-end and phase sample, batch wall, process I/O, S3 and service request
intervals, maximum observed concurrency, space, completion semantics, and
successful complete-byte checks.
