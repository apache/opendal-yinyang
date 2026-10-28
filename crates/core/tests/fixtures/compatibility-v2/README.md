# YYFILE02 compatibility fixtures

These fixtures are immutable inputs for the compatibility tests. The tests do
not regenerate them with the Rust encoder.

The vectors were generated against the byte descriptions in
`specs/content-profile.md`, `specs/yinyang-format.md`, and
`specs/metadata-service.md` at source revision
`fd1338c2f924a90e9da752b9158e748d1d3acaf4`. `generate.py` is an independent
encoder. Its binary portion ran with Python 3.13.18, PyPI `blake3==1.0.11`, and
SQLite 3.54.1. Its Unicode-name portion ran with Python 3.15.1 and Unicode
17.0.0. The exact producer output is stored in `binary-producer.txt` and
`unicode-producer.txt`. It does not call the Rust implementation or import its
crates. The committed files are the authority; tests load them with
`include_bytes!`.

Regenerate only when deliberately adding a new profile. A changed checksum for
an existing file means the claimed frozen profile changed:

```shell
python3.15 generate.py --names-only
python3.13 -m venv .venv
.venv/bin/pip install blake3==1.0.11
.venv/bin/python3.13 generate.py
sha256sum -c SHA256SUMS
```

The deterministic object fixture uses filesystem UUID
`11111111-1111-1111-1111-111111111111` and pack UUID
`22222222-2222-2222-2222-222222222222`. The root NodeId is sixteen `0x55`
bytes.

Current-profile files exercise successful reads. Files named `*-unknown.*` and
`*-malformed.*` exercise rejection before the affected resource is changed.
`service-current.db` and `service-unknown.db` are SQLite databases; the latter
contains a sentinel table used to prove that unsupported admission performs no
schema or journal-mode change.
