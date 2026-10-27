# Linux cost baseline: 2026-10-27

This records one bounded Linux product-path baseline for issue #78. It is not a
production capacity claim. The measured commit, binary digest, host, cache
policy, completion semantics, raw samples, and space series are retained in the
JSONL evidence.

## Tested identity

- Source: `c5c74589f61726100e6a0c5090e2c0c5b8ea6db2`
- Corrected service collector:
  `1302ceab5d29781d944e70e3e034248b94dac65f` on unchanged production code
- Release binary BLAKE3:
  `2e7cd592270e280cc5c2cd81fc3a28f8da38c0646107800c02cd4e9f12181da7`
- Host: 4 vCPUs, 15 GiB RAM, Ubuntu 24.04 kernel `7.0.0-1014-aws`
- Rust: `1.99.1`
- Backend: isolated loopback Silo, MinIO-compatible S3 over HTTP
- Runner label: `cost-c5c7458-full-r1`
- Runner result: `1 passed`, exit `0`
- Evidence:
  [linux-cost-2026-10-27-c5c7458.jsonl](benchmarks/linux-cost-2026-10-27-c5c7458.jsonl)
  (`129,999` bytes)
- Evidence JSONL SHA-256:
  `91eb5a0cd531f6f25060bf0e42887e348c6e35e0221727c4147a7936f538bff2`
- Corrected service evidence:
  [linux-cost-service-2026-10-27-1302cea.jsonl](benchmarks/linux-cost-service-2026-10-27-1302cea.jsonl)
  (`63,437` bytes), SHA-256
  `266c28e6eb90ca4c8a42ae0a585cd7cf8c375c07ce0013c40ac45fa4aefcc508`
- Corrected service runner label: `cost-1302cea-service-r1`, 22 measured and
  2 unsupported records, `1 passed`, exit `0`

The benchmark ran `cargo x bench-linux-cost --profile full` and produced 50
records: 48 measured records plus two unsupported service-authority one-shot
publish/restore combinations. Sync used 32 files (small) and 128 files (medium),
each 1 KiB. FUSE large-file workloads used 8 MiB and 32 MiB files, 4 KiB range
reads, and four sparse updates. Concurrent writers used two and four 256 KiB
files.

> **Service timing correction:** this raw c5c7458 evidence was collected with a
> counting service proxy that polled nonblocking accept with a 5 ms sleep.
> Because the current metadata transport opens one connection per RPC, that
> collector delay inflated service latency. Do not use service-authority p50
> values from this file. Service connection/request/byte counts and space
> observations remain inspectable, and the raw file is retained unchanged.
> The `1302cea` corrected service run supersedes service latency. Object rows
> remain the accepted `c5c7458` evidence.

## Representative results

Latency is the batch p50. Network values are per latency sample. Object and
service rows use separate collection paths and are not interchangeable.

| Workload | Authority | Collector | Scale | p50 | S3 requests/sample | Service RPCs/sample |
| --- | --- | --- | --- | ---: | ---: | ---: |
| Sync initial | Object | c5c | 32 files | 14.750 s | 2718 | 0 |
| Sync no-op | Object | c5c | 32 files | 3.216 s | 486 | 0 |
| Sync single edit | Object | c5c | 32 files | 3.449 s | 528 | 0 |
| Sync rename | Object | c5c | 32 files | 4.041 s | 631 | 0 |
| Sync initial | Service | 1302 | 128 files | 10.312 s | 897 | 3122 |
| Sync no-op | Service | 1302 | 128 files | 0.565 s | 1 | 857 |
| Sync single edit | Service | 1302 | 128 files | 0.666 s | 5.7 | 862.3 |
| Sync rename | Service | 1302 | 128 files | 0.636 s | 1 | 886 |
| FUSE cold open + 4 KiB read | Object | c5c | 8 MiB | 5.383 s | 1161 | 0 |
| FUSE cold open + 4 KiB read | Service | 1302 | 32 MiB | 22.533 s | 5632 | 7 |
| FUSE warm 4 KiB read | Object | c5c | 32 MiB | 0.096 ms | 0 | 0 |
| FUSE sparse update | Object | c5c | 32 MiB | 36.551 s | 6194 | 0 |
| FUSE sparse update | Service | 1302 | 32 MiB | 53.305 s | 7692 | 11 |
| Concurrent writers | Object | c5c | 4 writers | 0.462 s | 127.8 | 0 |
| Concurrent writers | Service | 1302 | 4 writers | 0.394 s | 18 | 18.2 |

Every cold FUSE read transferred approximately the complete file: 8.875 MiB of
S3 response for the 8 MiB file and 36.298 MiB for the 32 MiB file. Warm reads on
the same descriptor performed no storage request. Restart repeated the same cold
transfer. Replacement mount readiness was 25.649 ms for the 8 MiB fixture and
25.920 ms for the 32 MiB fixture; each row includes its own measured interval.

Object-authority one-shot publish took 2.199 s for 32 files and 12.182 s for 128
files. Restore took 1.896 s and 5.737 s. These commands support only the object
authority at this commit.

## Retained history and space

Four 4 KiB sparse updates retained a complete new content version each time.
Before any update, the object-authority 32 MiB fixture used `33,690,184` bytes
in 11 S3 objects. The first update raised that to `67,380,571` bytes in 17
objects; the fourth reached `168,455,998` bytes in 35 objects. Mount staging
stayed near 38 MiB. The service-authority fixture started at `33,688,423` bytes
in 2 S3 objects, reached `67,376,806` bytes in 3 objects after the first update,
and `168,441,955` bytes in 6 objects after the fourth. Its allocated SQLite
state remained about 40-48 KiB during the sequence.

For Sync, the measured space series covers each completed edit and rename. One
small object-authority edit increased S3 storage from 720,507 to 741,129 bytes
and object count from 327 to 337. Service metadata space is reported separately
as allocated SQLite database plus WAL/SHM bytes.

## Interpretation limits

The object authority performs metadata operations directly against Silo, while
the service authority reads metadata from SQLite over authenticated loopback
RPCs. Their latency and request counts therefore describe different deployed
configurations, not an implementation-only difference.

The service transport currently opens one TCP connection per RPC. Service RPC
counts are connection counts; the proxy counts raw frame bytes without decoding
the private protocol. S3 request counts come from parsed HTTP/1 requests to Silo.

Process I/O is sampled from `/proc/<pid>/io` and is a lower bound. Character I/O
includes sockets; block-layer bytes identify actual local disk work. The host
page cache was not dropped. FUSE uses `FOPEN_DIRECT_IO` and reclaims a clean node
on its last release. No claim is made about another host, backend, dataset shape,
or production duration.
