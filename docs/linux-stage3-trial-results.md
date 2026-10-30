# Linux stage-3 Amazon S3 trial result

Status: one bounded installed-target trial on 2026-10-31. This record supports
the named archive, script, workload, host, region and one unversioned bucket
only. It does not claim throughput, request count, production capacity,
reclamation eligibility or behavior on another backend.

## Sources and identities

- Product qualification source:
  `9014fbe9532727ba2e281394e26561eeb0e5825e`.
- Product source parents: D `7c5cfa1159e54ae67cfa7470342cebc3819358ad`
  and S `656ebdd886fb0cfefff32480708f7516ea5c91cd`.
- Archive:
  `yinyang-0.0.24-ubuntu-24.04-x86_64-9014fbe95327.tar.gz`.
- Archive SHA-256:
  `f717831c6205cfe563fdf6e0125380e7273b8e7c9c381ddc2e5aced6f463131d`.
- Release: `0.0.24+9014fbe95327`.
- Installed `yy` SHA-256:
  `bdfe412dc7351e478ed65bb74bf410c42614e468108f19fc0a27dc452ff9a923`.
- Python trial script source:
  `b6ae40c4b88523eda5a503e971db016ad1d99dab`.
- Python trial script SHA-256:
  `876c81b506df832a364f5fbb8763bcb04ab24f9ed0122fd0612ebfa4e005fc0d`.
- Genuine stage-3 producer:
  `546f419f9158e156f9a561822f3cb3f307222584`; producer archive SHA-256
  `771c94a5d5063ab2d61ac7a9896abfd2430d55baeb478bc9b3f97ae187790453`.

The installed archive source and the Python script source intentionally differ.
The script-only commits changed qualification coverage and evidence fields; they
did not alter or rebuild the `9014fbe` product binary. The raw report records
`source_matches_checkout=false` for the same reason.

## Environment and workload

- Ubuntu 24.04 x86_64 EC2 host, kernel
  `7.0.0-1014-aws`.
- Amazon S3, `us-east-2`, one dedicated unversioned general-purpose bucket.
- Public access blocked, temporary assumed role, credentials supplied only on
  the helper's stdin and never placed in arguments or files.
- `/dev/fuse`, `fuse.yinyang`, one installed archive binary.
- Seed `9014`.
- Initial content: `8 MiB + 17 B`, spanning multiple 64 KiB leaves.
- Append: `4 KiB`.
- Zero extension: `32 KiB`.
- Shrink to `4 KiB`, then extend to `68 KiB` to verify removed-byte zeroing.
- Clean unmount, remount, append `16 B`, clean unmount.
- Focused duration: `78.522 s`.

## Results

The trial exited `0` and both clean unmounts returned `0`.

- Initial-close bytes matched the expected payload.
- Append after `fsync`/close matched; `FileIO.write` returned all `4,096` bytes.
- Zero extension matched the complete expected bytes.
- Shrink-then-extend matched the complete expected zero-filled bytes.
- Remount restored the first-phase bytes and the remount append returned all
  `16` bytes.
- Independent append-stage `yy restore` read `8,392,721 B`, SHA-256
  `1820b8c78a65fb317cd676b3cd6b0fa1745d0c35d6f3dc9415c9dfd26a8127a5`.
- Independent final `yy restore` after the final clean unmount read `69,648 B`,
  SHA-256
  `be197b7ec41dd17a58d4f8605492e0de1517b529276ab793482d05062fa2c917`.
- Installed `doctor --json` reported
  `yinyang.operational-health.v1`, status `pass`.
- Exact prefix cleanup listed 40 keys and left 0 keys.

Observed seed-to-final space values:

| Measurement | Initial | Final | Delta |
| --- | ---: | ---: | ---: |
| Visible current objects | 16 | 40 | +24 |
| Visible current bytes | 8,425,675 | 8,555,956 | +130,281 |
| Physical retained revisions | 3 | 7 | +4 |
| Logical retained observed revisions | 3 | 7 | +4 |
| Logical retained file bytes | 8,388,625 | 25,346,115 | +16,957,490 |
| Logical live file bytes | 8,388,625 | 69,648 | -8,318,977 |

These are observations of current-key inventory and retained logical metadata.
The visible-key increase is not labeled as uploaded bytes, request count or
throughput. The logical live decrease follows the explicit truncation workload.
`noncurrent_versions` and external pins remain unknown,
`deletion_authorized=false`, and `reclaimable_bytes=null`.

The complete non-secret report is
[`benchmarks/linux-stage3-amazon-s3-9014fbe.json`](benchmarks/linux-stage3-amazon-s3-9014fbe.json),
SHA-256
`d0c708f4a560778f4c8b4fdba5cf9e798b864d8808b585dbd68bc559f30cce6c`.
It contains no credentials or role/account secrets.

## Boundary

This single run does not repeat the sustained operating-envelope matrix, the
producer fixture generation, or matched performance measurements. It does not
establish a performance result or authorize physical cleanup. The task bucket,
inline policy and standalone role were removed after independent empty-bucket
readback; the EC2 host and isolated Silo remain task infrastructure.
