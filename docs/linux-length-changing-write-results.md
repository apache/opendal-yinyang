# Linux length-changing write results

Date: 2026-10-31, Asia/Shanghai.

## Result

The trusted length-changing preparation and runtime remove complete-source
fallback from the measured Managed FUSE append, truncate, zero-extension, and
shrink-then-extend workflows through both publication authorities.

The common collector was schema `yinyang.linux-cost.v6`. Its collector,
xtask, and capture-script blobs were identical in both runs:

- `tests/linux_cost.rs`:
  `a0a8d21e3dabe3963b0d23bfe7451ad80836d853`
- `xtask/src/main.rs`:
  `2e5f7d318a9fa4f41623f3c8a4802afc51a914da`
- `tests/linux_incremental_cost_capture.sh`:
  `a7ddd956f58b66f14a6448b2cd753398c6281ca4`

| Run | Measured source | Production source | Binary SHA-256 | Binary BLAKE3 |
| --- | --- | --- | --- | --- |
| Baseline | `9c01df6e70f61363f008aef94f49014b43f5e176` | `346b460e972fec4517f3f2a7bd18912bc9341cc3` | `84e1fa8c3cabd068b0fe5296ca63155aa43ee440940c2f307de1571d749cca62` | `6d5e153b4d9a28edbaa248719fd5a5e36eedca45ce6462d7b624964d9f0f9e53` |
| Optimized | `9deed503e5491a7a7ec5c76647a18b28a662beec` | C `1a5b5c13a9ccbb0ae3ce23ec116a0f7d93d6031e`; W `2015693536ea32c06793fe3ca8b0b27dd2949a6f` | `caf631e4ec876cbe246a424acd35599779903875ce13f945c22cc062ea7b11e2` | `f49a2ebbe6fa13ea64b9e41dba7736bf1843aca2d97ed33114fd2b895ce55a2b` |

The baseline source differs from its production commit only in tests, xtask,
and documentation. The optimized measured source is the ordinary A merge of
the C and W candidates. The later W head changed documentation only.

Both runs used Linux `lance-fuse-demo`, kernel `7.0.0-1014-aws`, four CPUs,
Rust `1.99.1`, loopback Silo over HTTP, the same S3 and metadata-service
counting proxies, one 32 MiB file, and the same user workflow. Complete remote
bytes were checked outside every measured interval.

Operation rows have two sequential samples; mount and descriptor-open rows have
one. With `n=2`, the raw p95 equals the maximum and is not a stable tail
estimate. The p50 values below are descriptive observations for this bounded
run, not universal latency claims.

## Object publication

Network columns are totals over the listed samples.

| Scenario | n | Useful B/sample | p50 ms baseline -> optimized | S3 requests baseline -> optimized | GET body B baseline -> optimized | PUT body B baseline -> optimized |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mount ready | 1 | 0 | 51.08 -> 25.81 | 6 -> 6 | 1,654 -> 1,654 | 0 -> 0 |
| Shared reader open | 1 | 0 | 10.55 -> 9.40 | 7 -> 7 | 1,750 -> 1,750 | 0 -> 0 |
| Writer open | 1 | 0 | 0.09 -> 0.07 | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| 4 KiB append | 2 | 4,096 | 2,166.12 -> 46.79 | 4,183 -> 69 | 134,783,431 -> 29,899 | 67,394,711 -> 17,945 |
| 68 KiB boundary append | 2 | 69,632 | 2,086.40 -> 48.61 | 4,192 -> 75 | 135,085,905 -> 201,163 | 67,611,413 -> 169,042 |
| 4 KiB truncate | 2 | 4,096 | 2,134.20 -> 68.31 | 4,208 -> 80 | 135,357,003 -> 77,859 | 67,671,943 -> 32,371 |
| 4 KiB zero extension | 2 | 4,096 | 2,129.14 -> 72.11 | 4,208 -> 80 | 135,363,255 -> 84,111 | 67,683,059 -> 43,487 |
| Shrink then extend | 2 | 8,192 | 2,151.90 -> 77.39 | 4,213 -> 86 | 135,403,736 -> 124,785 | 67,702,367 -> 62,795 |
| Unchanged fsync | 2 | 0 | 4.25 -> 0.22 | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Restart ready | 1 | 0 | 25.81 -> 25.79 | 6 -> 6 | 1,654 -> 1,654 | 0 -> 0 |
| Restart tail read | 2 | 4,096 | 14.78 -> 13.86 | 14 -> 14 | 27,608 -> 27,608 | 0 -> 0 |

## Service publication

| Scenario | n | Useful B/sample | p50 ms baseline -> optimized | S3 requests baseline -> optimized | GET body B baseline -> optimized | PUT body B baseline -> optimized |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mount ready | 1 | 0 | 25.75 -> 25.71 | 1 -> 1 | 40 -> 40 | 0 -> 0 |
| Shared reader open | 1 | 0 | 14.73 -> 11.57 | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Writer open | 1 | 0 | 0.06 -> 0.05 | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| 4 KiB append | 2 | 4,096 | 3,535.56 -> 28.94 | 7,199 -> 9 | 202,160,280 -> 17,170 | 67,389,578 -> 12,812 |
| 68 KiB boundary append | 2 | 69,632 | 3,524.57 -> 32.62 | 7,217 -> 15 | 202,670,280 -> 182,182 | 67,603,356 -> 160,985 |
| 4 KiB truncate | 2 | 4,096 | 3,509.30 -> 32.41 | 7,230 -> 14 | 202,991,078 -> 50,972 | 67,660,962 -> 21,390 |
| 4 KiB zero extension | 2 | 4,096 | 3,481.90 -> 29.53 | 7,230 -> 14 | 202,999,270 -> 50,972 | 67,669,154 -> 29,582 |
| Shrink then extend | 2 | 8,192 | 3,517.47 -> 36.86 | 7,229 -> 14 | 203,048,229 -> 83,740 | 67,685,538 -> 45,966 |
| Unchanged fsync | 2 | 0 | 0.36 -> 0.25 | 0 -> 0 | 0 -> 0 | 0 -> 0 |
| Restart ready | 1 | 0 | 25.90 -> 25.82 | 1 -> 1 | 40 -> 40 | 0 -> 0 |
| Restart tail read | 2 | 4,096 | 14.21 -> 22.09 | 4 -> 4 | 25,031 -> 25,031 | 0 -> 0 |

The service connection counts for the five length-changing operations remained
12, 12, 16, 16, and 20 in both runs. The reduction occurred in backend S3
traffic rather than in a lower number of authenticated metadata RPCs.

## FUSE RELEASE lifecycle follow-up

The first PR124 CI run exposed an independent FUSE acceptance ordering failure:
an immediate fresh open after descriptor close read the old 4-byte pinned
version. Linux dispatches ordinary FUSE RELEASE work in the background, so
close returning does not prove the adapter received RELEASE.

The lifecycle fix removes a released kernel handle from the FUSE routing map
on the dispatch thread before scheduling clean reclamation. A request task that
already cloned the handle retains the documented Busy pin. Writable OPEN sends
the same inode attribute invalidation as read-only OPEN only after selecting
the Mount handle and applying `O_TRUNC`, before inserting the handle or
replying. The acceptance scenario waits for the no-lease runtime status to show
that staged writable handles were actually reclaimed before asserting the
fresh version. It does not treat close returning as RELEASE dispatch and does
not weaken the content assertion.

## Space and negative observations

| Observation | Baseline | Optimized |
| --- | ---: | ---: |
| Object staging after all edits | 737,280 B | 737,280 B |
| Service staging after all edits | 737,280 B | 737,280 B |
| Object visible remote bytes | 371,752,147 B | 34,014,294 B |
| Service visible remote bytes | 371,697,011 B | 33,959,158 B |
| Object remote objects | 71 | 71 |
| Service remote objects | 12 | 12 |

- Local mount staging did not shrink; both implementations already used bounded
  sparse local records. The measured improvement is remote preparation,
  upload, and retained-version traffic.
- Unchanged fsync emitted zero S3 requests, zero GET body bytes, and zero PUT
  body bytes in both runs. Its sub-millisecond latency difference is retained
  as noise-sensitive and is not a performance claim.
- The restart tail read used the same request and body counts in both runs.
  This change does not optimize the read path.
- Retained remote objects kept the same count because this milestone defines no
  physical garbage collection or history expiry. The reduced visible byte
  count is not deletion authorization.
- The service restart-read latency moved from `14.21` to `22.09` ms. This is a
  negative observation from one bounded host; it is not hidden or presented as
  improvement.

## Evidence

- [Baseline JSONL](benchmarks/linux-cost-length-changing-2026-10-31-baseline-9c01df6.jsonl):
  22 measured rows, SHA-256
  `d12c63d94fce7e489c55297a4ab6130d795536ab871ad14d6b67f91646a5885f`
- [Baseline binary identity](benchmarks/linux-cost-length-changing-2026-10-31-baseline-9c01df6.binary.txt)
- [Optimized JSONL](benchmarks/linux-cost-length-changing-2026-10-31-optimized-9deed50.jsonl):
  22 measured rows, SHA-256
  `221ee9079c8afbcc57d6daf9b24c772234fa0a47fd01876d860b57ef504f5093`
- [Optimized binary identity](benchmarks/linux-cost-length-changing-2026-10-31-optimized-9deed50.binary.txt)

The complete exact binary files remain in the task evidence directory outside
managed build storage. The raw JSONL retains every sample, per-sample network
share, process I/O, staging series, retained-storage series, source identity,
binary identity, and completion semantics.
