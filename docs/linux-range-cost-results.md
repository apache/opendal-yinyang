# Linux read-only range cost results: 2026-10-27

This records one matched read-only FUSE range workload for issue #78. It is not
a production capacity claim or a comparison with the earlier writable FUSE
fixtures.

## Tested identities

Both runs used the same collector, 8 MiB and 32 MiB files, 4 KiB and 8 KiB
ranges, loopback proxy chain, and read-only volume policy.

| Role | Source | Binary BLAKE3 | Runner |
| --- | --- | --- | --- |
| Pre-range baseline | `b8a931b8e223ad2967d7c36323b5fe172800175c` | `1e0dff5810a1f3921d3e6d76efab8f33223ea960cbc2cc4ca43bab5cf238d052` | `b8a931b-cost-ro-baseline-r1` |
| Read-only range | `a7abd0ee515033fbe60c1ef92b807009db75f2d9` | `54cfc9e9c523775497dc09b294eb5134c05e4f95dd7751be5bc2be6b0972d35c` | `a7abd0e-cost-ro-optimized-r1` |

The baseline collector commit changes only tests, xtask, and documentation
relative to `24fff0c49b9412a53750fc3cd95278b9d3bf865b`; its production source
is the pre-range implementation. The optimized source merges standalone range
head `1c83d3504337aac7d67ef84dceba34ad21045e76` onto that collector.

The host was the existing `lance-fuse-demo` AWS machine: four vCPUs, 15 GiB RAM,
Ubuntu 24.04, kernel `7.0.0-1014-aws`, and Rust `1.99.1`. The backend was
isolated loopback Silo, MinIO-compatible S3 over HTTP. Service publication used
the authenticated loopback TCP proxy and one connection per RPC.

Commands:

```shell
cargo x bench-linux-cost \
  --workload read-only-range \
  --output target/linux-cost/read-only-range-baseline.jsonl

cargo x bench-linux-cost \
  --workload read-only-range \
  --output target/linux-cost/read-only-range-optimized.jsonl
```

Raw evidence:

- [Baseline](benchmarks/linux-cost-read-only-range-2026-10-27-b8a931b.jsonl),
  20 records, 54,260 bytes, SHA-256
  `75bf45fea69b233a6ce3ebb79a15f9a4965118118c03ba9c53cc7f1aa915e475`
- [Optimized](benchmarks/linux-cost-read-only-range-2026-10-27-a7abd0e.jsonl),
  20 records, 53,877 bytes, SHA-256
  `c99f17af4e122c9f5df0ca48d0c05d6fda8aaa7e514f478661fe63d5a58e33f9`

## Cold and warm reads

Latency is the recorded p50. Cold rows contain one sample; warm rows contain
five sequential reads on one descriptor. Requests and response bytes are per
sample. Staging is the allocated directory size sampled while the descriptor
remained open.

| Authority | File | Workload | Baseline p50 | Range p50 | Baseline requests | Range requests | Baseline response | Range response | Baseline staging | Range staging |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Object | 8 MiB | cold 4 KiB | 5.088 s | 11.525 ms | 1159 | 16 | 8.872 MiB | 0.075 MiB | 10.668 MiB | 0.090 MiB |
| Object | 8 MiB | warm 4 KiB | 0.079 ms | 48.280 ms | 0 | 9 | 0 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Object | 32 MiB | cold 4 KiB | 22.846 s | 14.541 ms | 5639 | 18 | 36.296 MiB | 0.077 MiB | 33.934 MiB | 0.090 MiB |
| Object | 32 MiB | warm 4 KiB | 0.065 ms | 49.091 ms | 0 | 11 | 0 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |
| Service | 8 MiB | cold 4 KiB | 5.823 s | 12.726 ms | 1152 | 9 | 8.868 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Service | 8 MiB | warm 4 KiB | 0.112 ms | 48.029 ms | 0 | 9 | 0 MiB | 0.069 MiB | 10.668 MiB | 0.090 MiB |
| Service | 32 MiB | cold 4 KiB | 22.313 s | 14.499 ms | 5632 | 11 | 36.290 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |
| Service | 32 MiB | warm 4 KiB | 0.076 ms | 9.204 ms | 0 | 11 | 0 MiB | 0.071 MiB | 33.934 MiB | 0.090 MiB |

The optimized cold read fetched data and proof paths for the requested
verification units instead of materializing the complete file. For both file
sizes and authorities, response payload fell below 0.08 MiB per sample and
allocated mount staging fell from 10.668/33.934 MiB to 0.090 MiB.

The tradeoff is explicit in the warm rows. Repeated reads through the same
immutable `FileVersion` remain remote reads: each sample issued 9 to 11 S3
requests and transferred about 0.07 MiB, while baseline reads from complete
staging issued no requests. Warm latency therefore rose from approximately
0.1 ms to 9.2-49.1 ms. No cross-observation, persistent, or hidden content
cache is included in this result.

## Restart and boundary crossing

The restart mount became visible in 25.5-51.1 ms in both runs. After restart, a
cold 8 KiB read crossed a 64 KiB verification-unit boundary:

| Authority | File | Baseline p50 | Range p50 | Baseline requests | Range requests | Baseline response | Range response | Baseline staging | Range staging |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Object | 8 MiB | 5.143 s | 13.581 ms | 1159 | 18 | 8.872 MiB | 0.139 MiB | 12.094 MiB | 0.063 MiB |
| Object | 32 MiB | 23.380 s | 16.857 ms | 5639 | 20 | 36.296 MiB | 0.140 MiB | 36.336 MiB | 0.063 MiB |
| Service | 8 MiB | 5.221 s | 55.411 ms | 1152 | 11 | 8.868 MiB | 0.133 MiB | 12.097 MiB | 0.063 MiB |
| Service | 32 MiB | 23.882 s | 57.617 ms | 5632 | 13 | 36.290 MiB | 0.135 MiB | 36.336 MiB | 0.063 MiB |

The boundary-crossing read remained bounded by two verification units and proof
metadata rather than file length. The service cold boundary samples were slower
than the object samples, so the range payload reduction does not establish a
service latency improvement for every operation.

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
