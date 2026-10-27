# Linux cost baseline: 2026-10-27

This records one bounded Linux product-path baseline for issue #78. It is not a
production capacity claim. The measured commit, binary digest, host, cache
policy, completion semantics, raw samples, and space series are retained in the
JSONL evidence.

## Tested identity

- Source: `c5c74589f61726100e6a0c5090e2c0c5b8ea6db2`
- Release binary BLAKE3:
  `2e7cd592270e280cc5c2cd81fc3a28f8da38c0646107800c02cd4e9f12181da7`
- Host: 4 vCPUs, 15 GiB RAM, Ubuntu 24.04 kernel `7.0.0-1014-aws`
- Rust: `1.99.1`
- Backend: isolated loopback Silo, MinIO-compatible S3 over HTTP
- Runner label: `cost-c5c7458-full-r1`
- Runner result: `1 passed`, exit `0`
- Evidence JSONL SHA-256:
  `91eb5a0cd531f6f25060bf0e42887e348c6e35e0221727c4147a7936f538bff2`

The benchmark ran `cargo x bench-linux-cost --profile full` and produced 50
records: 48 measured records plus two unsupported service-authority one-shot
publish/restore combinations. Sync used 32 files (small) and 128 files (medium),
each 1 KiB. FUSE large-file workloads used 8 MiB and 32 MiB files, 4 KiB range
reads, and four sparse updates. Concurrent writers used two and four 256 KiB
files.

## Representative results

Latency is the batch p50. Network values are per latency sample. Object and
service rows use separate collection paths and are not interchangeable.

| Workload | Authority | Scale | p50 | S3 requests/sample | Service RPCs/sample |
| --- | --- | --- | ---: | ---: | ---: |
| Sync initial | Object | 32 files | 14.750 s | 2718 | 0 |
| Sync no-op | Object | 32 files | 3.216 s | 486 | 0 |
| Sync single edit | Object | 32 files | 3.449 s | 528 | 0 |
| Sync rename | Object | 32 files | 4.041 s | 631 | 0 |
| Sync initial | Service | 128 files | 22.285 s | 897 | 3122 |
| Sync no-op | Service | 128 files | 4.372 s | 1 | 857 |
| Sync single edit | Service | 128 files | 4.499 s | 5.7 | 862.3 |
| Sync rename | Service | 128 files | 4.561 s | 1 | 886 |
| FUSE cold open + 4 KiB read | Object | 8 MiB | 5.383 s | 1161 | 0 |
| FUSE cold open + 4 KiB read | Service | 32 MiB | 22.074 s | 5632 | 7 |
| FUSE warm 4 KiB read | Object | 32 MiB | 0.096 ms | 0 | 0 |
| FUSE sparse update | Object | 32 MiB | 36.551 s | 6194 | 0 |
| FUSE sparse update | Service | 32 MiB | 55.065 s | 7692 | 11 |
| Concurrent writers | Object | 4 writers | 0.462 s | 127.8 | 0 |
| Concurrent writers | Service | 4 writers | 0.561 s | 18 | 18.2 |

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
The object-authority 32 MiB fixture grew from `67,380,571` bytes in 17 S3 objects
to `168,455,998` bytes in 35 objects. Mount staging stayed near 38 MiB. The
service-authority fixture grew from `67,376,806` bytes in 3 S3 objects to
`168,441,955` bytes in 6 S3 objects; its allocated SQLite state remained about
40-48 KiB during the sequence.

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
