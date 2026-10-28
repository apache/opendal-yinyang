# Linux read-only Direct acceptance

`cargo x test-linux-direct` exercises an existing raw S3 namespace through the
real `yy --volume CONFIG mount` FUSE entry point. It does not first call
`yy create`, and every test compares a raw S3 inventory before and after the
operation.

The command requires:

- Linux with `/dev/fuse` and `fusermount3`;
- isolated plain-HTTP Silo `YINYANG_S3_*` settings;
- `YINYANG_DIRECT_VERSIONED_BUCKET`, an isolated bucket with S3 versioning
  enabled.

The harness uses its own path-style signed requests rather than OpenDAL's path
abstraction. Every seeded key is immediately fetched and listed again, so a
test cannot claim to cover a percent sign, DEL, trailing marker, case or
Unicode spelling that the client normalized before storage.

The Silo test endpoint rejects keys containing `.`/`..` components, empty path
components or NUL before they can be stored. Those reversible-mapping and
rejection rules are covered by the Direct mapper's deterministic unit tests;
the native FUSE fixture does not mislabel a normalized or rejected seed as
storage evidence.

Acceptance covers:

- Direct JSON with `read_only` omitted and with explicit `true`; explicit
  `false` is rejected by the actual mount entry point before any S3 connection;
- `capabilities` and `doctor` without Managed write primitives, staging state
  or authoritative YinYang metadata;
- complete-observation rejection for file-prefix conflicts, non-empty
  directory markers and overlong names that the selected backend can store;
- the reversible percent mapping while preserving case and Unicode
  normalization distinctions;
- raw external replacement and deletion, retained descriptor attributes,
  `SIGHUP`, and fresh observation;
- a true versioned bucket path that keeps an old descriptor on its exact
  version and an unversioned ETag path that fails `ESTALE` rather than falling
  back to an unconditional range read;
- SIGTERM teardown and a fresh mount without storage mutation.

The shared `YINYANG_S3_BUCKET` is never versioned by the suite. The separate
versioned bucket is a test resource and remains isolated from Managed suites.
