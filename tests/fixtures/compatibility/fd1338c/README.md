# fd1338c compatibility fixtures

This directory is immutable cross-version evidence. Do not regenerate or edit
these files with the current checkout.

The payloads were created by the `yy` release binary built from
`fd1338c2f924a90e9da752b9158e748d1d3acaf4`. The producer binary SHA-256 was
`4bc9270c20babb135873b83e9148eae078b9dadad3212e62d648bee41069a019`.
`manifest.json` records that source identity and hashes every payload with
SHA-256. `.gitattributes` keeps the fixture tree byte-exact on Windows
checkouts.

The generator is:

```shell
python3 tests/compatibility/generate_fd1338c.py \
  --legacy-binary /path/to/fd1338c/target/release/yy \
  --output tests/fixtures/compatibility/fd1338c \
  --producer fd1338c2f924a90e9da752b9158e748d1d3acaf4
```

It requires isolated plain-HTTP Silo `YINYANG_S3_*` settings and refuses any
producer other than `fd1338c`. The old binary owns all persisted formats:
object snapshots, receipts and retained history; SQLite service metadata;
Mount pending staging; Managed Sync and its backup package; and a committed
directory publication journal. SQLite state is copied with the backup API, so
no live `-wal`, `-shm` or process lease files are retained. Source trees and
expected identities are stored beside each payload for restore and recovery
assertions.

No storage credential, service token or command-line secret is included.
