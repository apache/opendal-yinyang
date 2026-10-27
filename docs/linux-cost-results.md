# Linux cost results: snapshot metadata reuse

This records the measured effect of issue #78's first production optimization:
an object-backed `Snapshot` reuses authenticated metadata pages and external
values for its own lifetime. It is not a production capacity claim or a promise
that the same result will hold for another authority, backend, dataset, or host.

> **Collector history:** this report's schema v1/v2 timings include the earlier
> S3 proxy accept polling and default Nagle relay behavior. They remain
> historical instrumented observations and must not be compared directly with
> schema v3 timings. Request, payload, and space evidence remains inspectable.
> See the corrected read-only range [control](linux-range-cost-results.md).

## Tested identities

| Role | Source | Binary BLAKE3 | Runner |
| --- | --- | --- | --- |
| Object baseline | `c5c74589f61726100e6a0c5090e2c0c5b8ea6db2` | `2e7cd592270e280cc5c2cd81fc3a28f8da38c0646107800c02cd4e9f12181da7` | `cost-c5c7458-full-r1` |
| Object optimized | `bcd5a308b8af779e6e786956703f1b9cd62d2a4c` | `97ed9e6af6b8b9e95ac6fcb6295432b42090e3dd005389b4eb3ee9720020d9de` | `cost-bcd5a30-full-r1` |
| Service baseline | `1302ceab5d29781d944e70e3e034248b94dac65f` | `2e7cd592270e280cc5c2cd81fc3a28f8da38c0646107800c02cd4e9f12181da7` | `cost-1302cea-service-r1` |
| Service optimized | `f18a3151fb76ef6dbf624a0abdde0d4d7fedca02` | `97ed9e6af6b8b9e95ac6fcb6295432b42090e3dd005389b4eb3ee9720020d9de` | `cost-f18a315-service-r1` |

The object pair preserves the same full-profile harness, workload, and timed
paths. Their production-code difference is the object Snapshot reuse; the
optimized source also includes reporting-only unsupported-summary rendering. The
service pair uses the corrected per-publication collector at both identities.
Its production service path is unchanged between baseline and optimized, while
each corrected service binary has the same BLAKE3 as its corresponding original
object run (baseline `2e7cd592...`, optimized `97ed9e6a...`). These cross-run
matches verify that the collector-only reruns did not change the measured binary.

Raw evidence:

- [Object baseline](benchmarks/linux-cost-2026-10-27-c5c7458.jsonl), 50 records,
  SHA-256 `91eb5a0cd531f6f25060bf0e42887e348c6e35e0221727c4147a7936f538bff2`
- [Object optimized](benchmarks/linux-cost-optimization-2026-10-27-bcd5a30.jsonl),
  50 records, SHA-256
  `66b99113e32b0402d9343792541878a2259e4e99e222f214ba3889e629fba4f1`
- [Corrected service baseline](benchmarks/linux-cost-service-2026-10-27-1302cea.jsonl),
  24 records, SHA-256
  `266c28e6eb90ca4c8a42ae0a585cd7cf8c375c07ce0013c40ac45fa4aefcc508`
- [Corrected service optimized](benchmarks/linux-cost-service-2026-10-27-f18a315.jsonl),
  24 records, SHA-256
  `458fa1338d9dd24d7bdd485948e90a3fcdd6d7ddef333623d17f059130dc790b`

The optimized full object file also contains service rows collected before the
service timing correction. Those rows are retained unchanged for provenance but
are superseded by the corrected service evidence. Use only its object rows for
the object comparison.

## Object authority

Latency is the batch p50. A request count is the S3 request count per latency
sample; no-op uses five samples, edit and rename use three, and one-shot rows use
one.

| Workload | Scale | Baseline p50 | Optimized p50 | Baseline requests | Optimized requests |
| --- | --- | ---: | ---: | ---: | ---: |
| Sync initial | 32 files | 14.750 s | 9.741 s | 2718 | 2049 |
| Sync no-op | 32 files | 3.216 s | 0.159 s | 486 | 20 |
| Sync single edit | 32 files | 3.449 s | 0.316 s | 528 | 60.7 |
| Sync rename | 32 files | 4.041 s | 0.612 s | 631 | 118 |
| Sync initial | 128 files | 88.552 s | 62.615 s | 14173 | 11012 |
| Sync no-op | 128 files | 16.906 s | 0.512 s | 1726 | 38 |
| Sync single edit | 128 files | 16.878 s | 0.808 s | 1768 | 78.7 |
| Sync rename | 128 files | 18.152 s | 1.174 s | 1876.3 | 141.7 |
| Restore | 32 files | 1.896 s | 0.239 s | 309 | 76 |
| Restore | 128 files | 5.737 s | 0.886 s | 1123 | 280 |

The demonstrated effect is fewer repeated metadata reads inside one pinned
observation. It is strongest for no-change and small-change Sync, where metadata
read amplification previously dominated. Initial publication still uploads and
acks the complete tree, so its improvement is smaller. Restore still reads all
restored bytes; its result reflects reduced index traversal, not content reuse.

## Unchanged and regressed paths

Object FUSE cold reads still materialize the complete file. The 8 MiB fixture
transferred `9,306,165` response bytes and took 5.383 s before, versus
`9,304,351` bytes and 5.619 s after. The 32 MiB fixture transferred
`38,060,996` bytes before and `38,059,182` bytes after, with p50 unchanged within
run-to-run variation. Warm reads remained local to staging. Four 4 KiB sparse
updates still retained and uploaded complete successive content versions.

The four-writer object sample moved from 0.462 s to 0.547 s p50. Two- and
four-writer samples overlap, have only two and four observations, and do not
isolate scheduling variance. This run does not establish a concurrency
improvement; the negative latency movement remains an explicit limitation.

## Service authority control

The optimization applies only to object-backed metadata. Corrected service rows
therefore act as a no-change control:

| Workload | Scale | Baseline p50 | Optimized p50 |
| --- | --- | ---: | ---: |
| Sync initial | 128 files | 10.312 s | 10.362 s |
| Sync no-op | 128 files | 0.565 s | 0.570 s |
| Sync single edit | 128 files | 0.666 s | 0.676 s |
| Sync rename | 128 files | 0.636 s | 0.651 s |

Request, connection, and byte accounting is identical between the corrected
service pair. These small differences are run variation and support neither a
service speedup nor a regression. They also separate the object metadata
amplification fix from the service's indexed SQLite records.

## Memory and lifetime

The cache is not persistent and is not shared across observations or processes.
Each Snapshot is bounded to 256 page payloads (16 MiB at the 64 KiB page limit)
and 4096 external values totaling at most 32 MiB. Cached page bytes are decoded
and validated again for every root or child use. A fresh observation starts cold.
See the
[transactional filesystem contract](../specs/transactional-filesystem.md).

## Limits

All measurements ran on one four-vCPU Linux host against an isolated loopback
Silo server. The host page cache was not dropped. Sample sizes are small, and
latency includes harness and process startup where each record specifies it.
Service observations open one TCP connection per RPC and are not comparable to
S3 request latency.

These results do not cover a production backend, network latency, other
datasets, long-running operation, native platform acceptance, or release
qualification. Raw JSONL retains latency samples, process I/O, space series,
cache policy, completion boundaries, source identities, and binary hashes.
