# Verified immutable range reuse: 2026-10-28

This records one matched repeated/random read study for issue #78. It compares
the unchanged Managed read-only range reader with a bounded process-local cache
for fully authenticated immutable-version ranges. The backend was real Amazon
S3, not loopback Silo.

## Tested identities

| Role | Source | Binary BLAKE3 | Range policy |
| --- | --- | --- | --- |
| Uncached baseline | `efe69f7ca0fbae6217e659a77cdeb321c9151ed7` | `de586e69195577d975de87a3cb9c28ca4433c8a00512637ea3eee07797fc2a9a` | `none-open-0.59.5-production-range-reader` |
| Bounded reuse | `50f44d04907b0e65824f38f8482cdca69bf83cd7` | `5cdd3888826325da8e56e47900caa5c15dc3a6187e1bf4b49bef05d6468acdfb` | per-DataStore 16 MiB / 4096 entry LRU keyed by exact descriptor and `PackedRef` |

Both trees used OpenDAL `0.59.5`, the schema v4 Linux cost collector, a clean
worktree, and the same `benchmark-telemetry` binary feature. The baseline commit
is the telemetry-enabled harness immediately before the cache implementation.
The optimized tree adds the cache and post-batch reuse assertions; timed
workload bodies use the same FUSE operations, dataset generator, read offsets,
sample counts, telemetry, and settlement rules.

The host was `lance-fuse-demo`: four vCPUs, 15 GiB RAM, Ubuntu 24.04, kernel
`7.0.0-1014-aws`, and Rust `1.99.1`. Storage was an encrypted, public-access-
blocked Amazon S3 bucket in `us-east-2`, reached directly over HTTPS through an
assumed, bucket-scoped task role. There was no S3 proxy or TLS termination.
Baseline and optimized runs used disjoint task roots below
`linux-next-2026-10-28/`.

## Workload

Each scale created the same deterministic 8 MiB or 32 MiB payload, published it
through the Managed object authority, and mounted it through a read-only Linux
FUSE volume. The workload used one `FOPEN_DIRECT_IO` descriptor unless a phase
explicitly tested restart or replacement:

- one cold open plus a 4 KiB midpoint read;
- 32 repeated 4 KiB reads at that offset;
- one 8 KiB read crossing a 64 KiB verification-unit boundary, then 32 repeats;
- 128 deterministic random 4 KiB reads covering an 8 MiB hot working set, then
  32 reads from that set;
- for 32 MiB only, 288 deterministic random reads across the complete file;
- unmount/remount and a cold midpoint read;
- for 32 MiB only, eight sentinel reads, 272 distinct units (17 MiB) of eviction
  pressure, then eight sentinel rereads;
- an old descriptor read before replacement, a fresh open after path removal,
  recreation and SIGHUP, then another read through the retained old descriptor.

The shuffled unit order and unit-local offset used fixed seed
`0x79594d32373732`. Cold measurements contain one sample. Warm batches contain
32 samples, except the eviction sentinel batches (eight), old-descriptor batches
(two), and the 128/272/288 deterministic random batches.

## Accounting

The measured `yy` process installed the released OpenDAL MetricsLayer before
building its volume operator. HTTP request counts and response body bytes were
therefore observed in-process without decrypting TLS or inserting a proxy.
Request body bytes are zero for these GETs. Counts exclude HTTP headers and TLS
records; host network counters are not substituted for request counts.

Latency is client-observed FUSE `pread` latency. Network counters are settled
150 ms after each latency batch, outside the measured interval. A warm assertion
requires zero HTTP requests for the same-offset, boundary, and hot-set batches.
RSS/HWM comes from `/proc/PID/status` and is process-wide, not an exact cache
accountant. The 16 MiB limit covers cached payload bytes; hash-map, ordering,
key, allocator, and in-flight buffers add overhead. Staging is allocated mount
directory space while the descriptor remains mounted.

## Results

Warm reads through the same immutable version no longer touch S3:

| Scale | Workload | Baseline p50 | Reuse p50 | Baseline requests/read | Reuse requests/read |
| --- | --- | ---: | ---: | ---: | ---: |
| 8 MiB | repeated 4 KiB | 217.853 ms | 0.053 ms | 9 | 0 |
| 32 MiB | repeated 4 KiB | 291.127 ms | 0.054 ms | 11 | 0 |
| 8 MiB | boundary warm 8 KiB | 267.919 ms | 0.098 ms | 11 | 0 |
| 32 MiB | boundary warm 8 KiB | 330.820 ms | 0.067 ms | 13 | 0 |
| 8 MiB | 8 MiB hot-set warm | 220.283 ms | 0.223 ms | 9 | 0 |
| 32 MiB | 8 MiB hot-set warm | 279.974 ms | 0.058 ms | 11 | 0 |

Baseline warm reads transferred about 65.4 KiB (8 MiB file) or 65.8 KiB
(32 MiB file) of response body per 4 KiB read; reuse reads transferred zero.
The shortest local p95 values in the same batches were 0.274-0.477 ms.

Cold and bounded-capacity behavior remains visible:

| Scale | Workload | Baseline p50 | Reuse p50 | Baseline requests/sample | Reuse requests/sample |
| --- | --- | ---: | ---: | ---: | ---: |
| 8 MiB | cold open + 4 KiB | 380.588 ms | 379.094 ms | 16 | 16 |
| 32 MiB | cold open + 4 KiB | 447.285 ms | 447.363 ms | 18 | 18 |
| 8 MiB | random hot-set fill | 223.085 ms | 62.301 ms | 9 | 2.84 |
| 32 MiB | random hot-set fill | 272.222 ms | 89.497 ms | 11 | 2.91 |
| 32 MiB | full-file random, 288 reads | 278.089 ms | 73.914 ms | 11 | 2.76 |
| 8 MiB | restart cold 4 KiB | 388.523 ms | 390.144 ms | 16 | 16 |
| 32 MiB | restart cold 4 KiB | 446.448 ms | 451.849 ms | 18 | 18 |

The fill batches include units already read by earlier phases. The optimized
full-file random batch exceeds the 16 MiB payload bound and still issues 2.76
requests per read; this is not an all-file cache claim.

The boundary "cold" row also reuses proof nodes from the earlier same-version
reads. Its optimized request count therefore fell from 15/17 to 10/12 for the
8/32 MiB files even though both boundary data units were first accessed in that
phase.

Forced eviction reloads remote data. After 17 MiB of distinct-unit pressure, the
first sentinel reread took 303.573 ms and accounted for the batch's six HTTP
requests; the following seven rereads were cache hits, giving a misleadingly
small p50 of 0.288 ms. Before pressure, the first sentinel read took 296.280 ms
and the later reads were hits. Restart also begins with an empty process-local
cache.

Replacement semantics remained exact. A fresh descriptor after remove,
recreate and SIGHUP issued 12 requests and read the new bytes. The retained old
descriptor still read the old version; its later warmed read issued zero
requests. A fresh logical version did not reuse the old version's cache entry.

## Memory and negative costs

Mount staging stayed at 0.062 MiB in both trees and all phases. Process peak RSS
was 20,520 KiB in the baseline run and 42,764 KiB in the reuse run, an observed
increase of 22,244 KiB (21.7 MiB) after over-capacity pressure. The 8 MiB
hot-set phase reached 29,312 KiB versus 20,480 KiB, an 8.6 MiB process increase.
These values include allocator behavior and benchmark-side vectors; they do not
claim that exactly 16 MiB was resident in cache.

Cold open/read and restart latencies were effectively unchanged. Fresh
descriptor reads correctly missed and showed one-sample p50 noise in both
directions. Over-capacity random access retained partial benefit but remained
remote-bound. First reads after eviction remained full storage reads.

Previously verified cached bytes can outlive source-descriptor release until
LRU eviction or process exit, and can remain readable after external deletion
while cached. Proof and data hashes are rechecked against the pinned descriptor
on every hit, but a cached physical range is not fetched again. External
mutation remains outside the immutable-backend contract.

## Evidence

- [Baseline JSONL](benchmarks/linux-cost-repeated-random-2026-10-28-baseline-efe69f7.jsonl),
  28 records, SHA-256
  `68acb84f9bcd72657371cc4872cf7f5f998cc80c24a5da778d8b5bd8fa8b638b`
- [Bounded-reuse JSONL](benchmarks/linux-cost-repeated-random-2026-10-28-verified-reuse-50f44d0.jsonl),
  28 records, SHA-256
  `18f6b5c033c4d5b134b5ed61238c14b7f028ced3d5e9cd139638618add1b76b9`
- Final per-process telemetry examples preserve the
  `yinyang.benchmark-http.v1` schema; JSONL records contain the phase totals used
  above:
  [baseline small](benchmarks/linux-cost-repeated-random-2026-10-28-baseline-telemetry-small.json),
  [baseline medium](benchmarks/linux-cost-repeated-random-2026-10-28-baseline-telemetry-medium.json),
  [reuse small](benchmarks/linux-cost-repeated-random-2026-10-28-verified-reuse-telemetry-small.json),
  and [reuse medium](benchmarks/linux-cost-repeated-random-2026-10-28-verified-reuse-telemetry-medium.json).

The exact credential-bearing commands are intentionally not retained. The
reusable wrapper `tests/linux_verified_range_reuse_real_s3.py` reads STS JSON
from stdin, passes values only through child environments, and redacts every
credential value from captured output.

## Limits

This is one sequential, short-running, same-region host/backend sample. It does
not establish cross-region latency, concurrent reader scaling, production
capacity, long-running memory behavior, multi-file cache contention, or a
persistent/cross-process cache. Cold phases have one sample. The study does not
change the 64 KiB verification unit or any persisted format.
