# Linux read-only range cost results: 2026-10-27

This records one matched read-only FUSE range workload for issue #78. It is not
a production capacity claim or a comparison with the earlier writable FUSE
fixtures.

## Tested identities

Both runs used the same schema v3 collector, 8 MiB and 32 MiB files, 4 KiB and
8 KiB ranges, loopback proxy chain, and read-only volume policy.

| Role | Source | Binary BLAKE3 | Runner |
| --- | --- | --- | --- |
| Pre-range baseline | `5dff98ba222be690a7303f0c38b87514bf1c55e3` | `1e0dff5810a1f3921d3e6d76efab8f33223ea960cbc2cc4ca43bab5cf238d052` | `5dff98b-cost-ro-baseline-corrected-r1` |
| Read-only range | `cecc141ad62939d38bf503c474a9b3cb2b751ebe` | `4dde73ed20ee6d0e7af77b4e0826b78c7f62dad5ee2078ce392ba75494ed64e2` | `cecc141-cost-ro-corrected-r1` |

The baseline collector commit changes only tests, xtask, and documentation
relative to `24fff0c49b9412a53750fc3cd95278b9d3bf865b`; its production source
is the pre-range implementation. The optimized collector merges final frozen
range documentation at `69f6b99bf68eef91124d341fafb83425b81469d8` onto that
collector. Its code-bearing range head is
`895fe8dc75eef3340c52f50970c89b0c8511c84d`.

The host was the existing `lance-fuse-demo` AWS machine: four vCPUs, 15 GiB RAM,
Ubuntu 24.04, kernel `7.0.0-1014-aws`, and Rust `1.99.1`. The backend was
isolated loopback Silo, MinIO-compatible S3 over HTTP. Service publication used
the authenticated loopback TCP proxy and one connection per RPC.

## Collector correction

The first evidence used a counting proxy whose S3 accept loop polled every
5 ms and whose relay sockets left Nagle enabled. Its raw `cache_policy` field
also described the optimized `FileVersion` path for both baseline and optimized
rows; that claim was wrong for the pre-range baseline.

Schema v3 replaces polling with blocking accept and sets `TCP_NODELAY` on both
relay legs. The corrected control changed small-read latency materially, so
both production identities were rerun with the same collector. The original
schema v2 JSONL files remain unchanged for provenance, but their latency and
`cache_policy` values are superseded.

Commands:

```shell
cargo x bench-linux-cost \
  --workload read-only-range \
  --output target/linux-cost/read-only-range-baseline-corrected-r1.jsonl

cargo x bench-linux-cost \
  --workload read-only-range \
  --output target/linux-cost/read-only-range-corrected-r1.jsonl
```

Raw evidence:

- [Corrected baseline](benchmarks/linux-cost-read-only-range-2026-10-27-baseline-v3.jsonl),
  20 records, 53,930 bytes, SHA-256
  `c96c0ef4bfc993d49a13ec54122bf43b1eafe5f44e5ce535bac8e55c4361f6ec`
- [Corrected optimized](benchmarks/linux-cost-read-only-range-2026-10-27-optimized-v3.jsonl),
  20 records, 53,538 bytes, SHA-256
  `b57f1deacceb847d2e4a02a7678f4f14176157e4e506fee3197b2fc3b701533f`

The superseded schema v2 files are
[baseline](benchmarks/linux-cost-read-only-range-2026-10-27-b8a931b.jsonl),
[pre-attribute-fix optimized](benchmarks/linux-cost-read-only-range-2026-10-27-pre-attribute-fix.jsonl),
and [attribute-fixed optimized](benchmarks/linux-cost-read-only-range-2026-10-27-af4b591.jsonl).

## Cold and warm reads

Latency is the recorded p50. Cold rows contain one sample; warm rows contain
five sequential reads on one descriptor. Requests and response bytes are per
sample. Staging is the allocated directory size sampled while the descriptor
remained open.

| Authority | File | Workload | Baseline p50 | Range p50 | Baseline requests | Range requests | Baseline response | Range response | Baseline staging | Range staging |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Object | 8 MiB | cold 4 KiB | 1.159 s | 13.147 ms | 1159 | 16 | 8.872 MiB | 0.075 MiB | 10.668 MiB | 0.090 MiB |
| Object | 8 MiB | warm 4 KiB | 0.081 ms | 8.085 ms | 0 | 9 | 0 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Object | 32 MiB | cold 4 KiB | 5.859 s | 14.234 ms | 5639 | 18 | 36.296 MiB | 0.077 MiB | 33.934 MiB | 0.090 MiB |
| Object | 32 MiB | warm 4 KiB | 0.080 ms | 9.134 ms | 0 | 11 | 0 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |
| Service | 8 MiB | cold 4 KiB | 1.414 s | 12.775 ms | 1152 | 9 | 8.868 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Service | 8 MiB | warm 4 KiB | 0.504 ms | 7.405 ms | 0 | 9 | 0 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Service | 32 MiB | cold 4 KiB | 6.812 s | 16.352 ms | 5632 | 11 | 36.290 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |
| Service | 32 MiB | warm 4 KiB | 0.074 ms | 9.178 ms | 0 | 11 | 0 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |

The optimized cold read fetched data and proof paths for the requested
verification units instead of materializing the complete file. For both file
sizes and authorities, response payload fell below 0.08 MiB per sample and
allocated mount staging fell from 10.668/33.934 MiB to 0.090 MiB.

The tradeoff is explicit in the warm rows. Repeated reads through the same
immutable `FileVersion` remain remote reads: each sample issued 9 to 11 S3
requests and transferred about 0.07 MiB, while baseline reads from complete
staging issued no requests. Warm p50 latency therefore moved from 0.07-0.50 ms
to 7.4-9.2 ms. No cross-observation, persistent, or hidden content cache is
included in this result.

## Restart and boundary crossing

The restart mount became visible in 25.5-51.0 ms in both runs. After restart, a
cold 8 KiB read crossed a 64 KiB verification-unit boundary:

| Authority | File | Baseline p50 | Range p50 | Baseline requests | Range requests | Baseline response | Range response | Baseline staging | Range staging |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Object | 8 MiB | 1.159 s | 14.638 ms | 1159 | 18 | 8.872 MiB | 0.139 MiB | 12.094 MiB | 0.063 MiB |
| Object | 32 MiB | 6.851 s | 15.686 ms | 5639 | 20 | 36.296 MiB | 0.140 MiB | 36.337 MiB | 0.063 MiB |
| Service | 8 MiB | 1.491 s | 14.978 ms | 1152 | 11 | 8.868 MiB | 0.133 MiB | 12.097 MiB | 0.063 MiB |
| Service | 32 MiB | 6.691 s | 16.004 ms | 5632 | 13 | 36.290 MiB | 0.135 MiB | 36.337 MiB | 0.063 MiB |

The boundary-crossing read remained bounded by two verification units and proof
metadata rather than file length. The single-sample cold and boundary latency
values do not establish a distribution or service-specific latency advantage.

## Limits

Cold measurements contain one sample and therefore do not establish a latency
distribution. Warm measurements contain five sequential samples on one host and
one backend. The run did not drop the host page cache, but the read-only range
path does not reuse staged file pages. Process I/O and network counters remain
lower bounds or proxy observations as defined in the raw JSONL records.

These results cover loopback Silo only. They do not establish non-loopback
network performance, production concurrency, other datasets, long-running
behavior, or release qualification. Native FUSE acceptance and operational
recovery remain separate gates.
