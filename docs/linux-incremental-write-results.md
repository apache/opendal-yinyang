# Linux incremental write results

Date: 2026-10-29, Asia/Shanghai.

## Result

The matched trusted-delta plus sparse-runtime implementation removes full-file
materialization and preparation from bounded same-length writable FUSE updates
through both Managed authorities. Each recorded scenario has one latency
sample, so this report gives elapsed time rather than a p50/p95 estimate or a
stable performance claim.

The common collector blob was
`6e671f3829205979bf72f0e14d41fd562f7ba39f`; the xtask blob was
`63d2c78aa6522e9bfafe654befca5525f7fcd6db`.

| Run | Source | Binary SHA-256 | Binary BLAKE3 |
| --- | --- | --- | --- |
| Baseline | `1520112d44bbaa9172eb495ef79c6ceaa6256b02` | `a6724f23fd5b5120f14b17dd9290db2cd337ebe8f934758accc953317f3c154c` | `634dfe14945b714eca47e726257a61444f8e773e84842debc9d2e7e6dbcb361b` |
| Optimized | `52e166b6bf8dfa8d2dd0ae60d5c174d444ff7929` | `e068a46ad2c69d6c55310a21f094ea577cf4cb3d2845acb5f92f19194b2d4eee` | `937d65af14f99da0472dfdd9cc1a246d0b0b73898a0908b0e8eb076c3189a313` |

The baseline branch changes only the benchmark collector and xtask over
production source `998c11f529650af6c8f62197c532406db0613ff1`. The optimized
source contains C `91cae44d9245424c401bce35d66190f67f9d7cda` and W
`6d9180df7a1dbf97c1f8ff0f844025bdbfb8cb7c`. Later S/D/A integration changes
do not alter this measured fast path, so the accepted comparison was not rerun.

Both runs used:

- Linux `lance-fuse-demo`, kernel `7.0.0-1014-aws`, four CPUs, and
  16,078,380 KiB memory;
- Rust `1.99.1 (58cf8f9c3 2026-10-13)` and mbx-managed release builds;
- loopback Silo, MinIO-compatible S3 over HTTP;
- a loopback HTTP/1 counting proxy outside `yy`, with GET response-body and
  PUT request-body bytes counted separately from headers;
- a second metadata-service proxy while the storage proxy observes backend work
  performed inside the service process;
- one 32 MiB file, writable open, three same-length overwrites, unchanged
  fsync, remount, and a 128 KiB read across the final edits; and
- complete remote-byte comparisons outside each measured interval.

## Elapsed and backend work

| Authority | Scenario | Baseline elapsed ms | Optimized elapsed ms | Baseline GET body B | Optimized GET body B | Baseline PUT body B | Optimized PUT body B |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| object | writable open | 3135.950 | 9.706 | 33,690,117 | 1,734 | 0 | 0 |
| object | unit-center overwrite + fsync | 727.573 | 52.135 | 33,693,107 | 140,231 | 33,690,568 | 69,527 |
| object | unit-boundary overwrite + fsync | 660.673 | 55.937 | 33,694,654 | 272,988 | 33,691,291 | 135,855 |
| object | EOF overwrite + fsync | 664.240 | 49.966 | 33,696,201 | 143,325 | 33,692,014 | 70,973 |
| object | restart + 128 KiB read | 3754.515 | 19.011 | 33,690,117 | 134,681 | 0 | 0 |
| service | writable open | 3702.864 | 9.461 | 33,688,383 | 0 | 0 | 0 |
| service | unit-center overwrite + fsync | 2027.143 | 38.997 | 67,376,766 | 134,684 | 33,688,383 | 67,342 |
| service | unit-boundary overwrite + fsync | 2036.489 | 40.734 | 67,376,766 | 265,894 | 33,688,383 | 132,947 |
| service | EOF overwrite + fsync | 2012.238 | 36.655 | 67,376,766 | 134,684 | 33,688,383 | 67,342 |
| service | restart + 128 KiB read | 3380.855 | 19.968 | 33,688,383 | 132,947 | 0 | 0 |

The service baseline GET count is larger because the service process performed
the full-file backend reads that were hidden from mount-to-service RPC counts.
The optimized required mode rejects a writable open or fsync interval that reads
or writes at least half of the file.

## Local and retained space

| Authority | Observation | Baseline | Optimized |
| --- | --- | ---: | ---: |
| object | allocated mount staging after open | 35,647,488 B | 110,592 B |
| object | allocated mount staging after three edits | 35,647,488 B | 323,584 B |
| object | visible remote bytes after three edits | 134,763,834 B | 33,966,316 B |
| service | allocated mount staging after open | 35,651,584 B | 110,592 B |
| service | allocated mount staging after three edits | 35,651,584 B | 319,488 B |
| service | visible remote bytes after three edits | 134,753,572 B | 33,956,054 B |

The fixture started at `33,690,432` remote bytes for object and `33,688,423`
for service. The optimized runs retained `275,884` and `267,631` additional
visible bytes for three updates. This growth is a real tradeoff and is not
reported as reclaimable space or deletion authorization.

## Bounds and negative observations

- An unchanged fsync emitted zero backend GET or PUT in both runs.
- Its single-sample elapsed time moved from `0.254` to `0.786 ms` for object and
  `0.225` to `0.240 ms` for service. These sub-millisecond changes are retained
  as noise-sensitive negative observations, not a regression claim.
- Mount readiness stayed near `25.7-25.9 ms`; the optimization does not claim
  faster FUSE process startup.
- Length-changing writes may still use the documented stream fallback. This
  workload asserts only the same-length fast path.
- This is one bounded loopback workload on one host. It is not a production
  capacity, cross-backend, zero-growth, or constant-latency claim.

## Evidence

- [Baseline JSONL](benchmarks/linux-cost-incremental-2026-10-29-baseline-1520112.jsonl),
  16 rows, SHA-256
  `a354a15aa4bbe46b6a99a94115852ce565d9a6411537906799545bbadfecc0d4`
- [Optimized JSONL](benchmarks/linux-cost-incremental-2026-10-29-optimized-52e166b.jsonl),
  16 rows, SHA-256
  `e52da254536c790c614c766ba22e09e32fc3021a14d4ecd54a48f2bd9a99a271`

The exact binary files remain in the task evidence directory outside managed
build storage. The raw JSONL includes every elapsed sample, source and binary
identity, process I/O sample, method/body byte count, staging series, retained
storage series, completion semantics, and successful invariant.
