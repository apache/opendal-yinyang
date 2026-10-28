# Linux read-only Direct acceptance

`cargo x test-linux-direct` exercises an existing raw S3 namespace through the
real `yy --volume CONFIG mount` FUSE entry point. It does not first call
`yy create`. Native backend tests compare the visible raw S3 inventory and
every seeded key before and after the operation.

The command requires:

- Linux with `/dev/fuse` and `fusermount3`;
- isolated plain-HTTP Silo `YINYANG_S3_*` settings;
- `YINYANG_DIRECT_VERSIONED_BUCKET`, an isolated bucket with S3 versioning
  enabled.

The harness uses its own path-style signed requests rather than OpenDAL's path
abstraction. Every seeded key is immediately fetched by exact GET, and native
cases compare the backend's actual LIST inventory. The controlled complete-list
collision fixture is the exception: it verifies both exact keys and deliberately
supplies the protocol response because the selected backend omits one.

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
- complete-observation rejection for non-empty directory markers and overlong
  names that the selected backend lists;
- a controlled complete `ListObjectsV2` response containing both `a` and
  `a/b`, routed through the actual CLI and OpenDAL request path, which must
  reject the file-prefix collision after that single GET and before mounting;
- the reversible percent mapping while preserving case and Unicode
  normalization distinctions;
- raw external replacement and deletion, retained descriptor attributes,
  `SIGHUP`, and fresh observation;
- a true versioned bucket path that keeps an old descriptor on its exact
  version and an unversioned ETag path that fails `ESTALE` rather than falling
  back to an unconditional range read;
- SIGTERM teardown and a fresh mount without storage mutation.

The recursive LIST must return every current object key exactly; it is a
backend contract prerequisite for Direct. Pagination is not an atomic snapshot,
because external writers can make different pages describe different moments.
The selected Silo test endpoint supports exact GET for both `a` and `a/b`, but
its `ListObjectsV2` response exposes only `a`. Direct cannot discover the
hidden key without guessing names or probing arbitrary paths, so this scenario
is outside the validated Direct support boundary. The controlled complete-list
fixture proves collision rejection without claiming that Silo qualifies; the
remaining backend qualification stays open.

The shared `YINYANG_S3_BUCKET` is never versioned by the suite. The separate
versioned bucket is a test resource and remains isolated from Managed suites.

## Amazon S3 backend qualification

The default `cargo x test-linux-direct` command uses isolated Silo so it can
run without cloud credentials. It does not qualify Amazon S3's complete
enumeration, TLS endpoint, version retention or conditional mutation
primitives. Those properties require the explicit real-Amazon-S3 suite:

```shell
python3 tests/linux_direct_real_s3.py
```

The helper reads one STS Credentials JSON document from stdin. It accepts
either the bare `Credentials` object or a wrapping `{"Credentials": ...}`
document. It then removes inherited `YINYANG_*` and AWS endpoint settings,
installs only short-lived credentials in the child environment and runs:

```text
cargo test --locked -p yinyang --test linux_direct direct_amazon_s3_
  -- --ignored --nocapture --test-threads=1
```

An optional argument beginning with `direct_amazon_s3_` restricts a targeted
rerun to one qualified scenario while preserving the same credential and
endpoint checks.

Do not put credentials in environment assignments visible in a command line,
files or logs. A qualification invocation through the shared Linux runner
looks like:

```shell
aws sts assume-role ... --duration-seconds 7200 --output json \
  | ssh <linux-host> \
      'cd <pinned-worktree> && \
       YINYANG_DIRECT_UNVERSIONED_BUCKET=<plain-bucket> \
       YINYANG_DIRECT_VERSIONED_BUCKET=<versioned-bucket> \
       YINYANG_QUALIFICATION_REGION=us-east-2 \
       AWS_CLI_BIN=/home/ubuntu/.local/bin/aws \
       bash <validation-runner> <unique-label> \
       python3 tests/linux_direct_real_s3.py'
```

The two buckets must be task-owned, in the same region and distinct:

- the unversioned bucket has never enabled Versioning;
- the versioned bucket has `Status=Enabled` before the suite starts.

The default test does not set `YINYANG_S3_ENDPOINT`, `AWS_ENDPOINT_URL` or
`AWS_ENDPOINT_URL_S3`; it must use the provider's Amazon S3 endpoint. The
tests reject an endpoint override instead of treating a proxy or compatible
service as Amazon S3 evidence.

Fixture writes and exact GET/LIST observations use the official AWS CLI
outside the mount. The mounted `yy` process is the only component exercising
the Direct implementation. Coverage includes:

- raw `ListObjectsV2` continuation across more than 1000 keys, followed by
  exact GET and a successful FUSE mount that exposes the complete set;
- a second namespace where `a` and `a/b` are both returned by raw LIST and
  `a/b` appears only on a later page; the actual mount must reject the
  file-prefix collision without changing either object;
- reversible percent mapping, DEL, spaces, case distinctions and composed
  versus decomposed Unicode names, with successful exact GET and FUSE reads;
- Versioning-enabled version IDs, `ListObjectVersions` retention, replacement,
  delete markers, pinned old descriptors, `SIGHUP`, `fstat` and fresh opens;
- the never-versioned bucket's ETag-pinned path, including bounded failure
  after external replacement or deletion rather than an unconditional read;
- `If-None-Match: *` creates, `If-Match` replacements, concurrent competing
  precondition requests, and conditional DELETE using the current ETag;
- stale conditional DELETE leaving the replacement intact, successful
  conditional DELETE, and explicit missing-key or delete-marker behavior;
- raw inventory equality around read-only mounts, refused writable opens,
  SIGTERM teardown and clean remount.

The tests use small objects and delete their object prefixes and versions after
each case. The task-owned buckets and temporary assumable role remain until all
qualification consumers finish; deleting them is a separate coordinator
decision. A report recording the exact source, binary, host, bucket names,
role ARN, AWS CLI version, test commands and observed limitations belongs with
the qualification evidence.

This matrix covers Amazon S3 general-purpose buckets with default regional
endpoints in us-east-2 and the recorded temporary role policy. Buckets with
suspended versioning, Requester Pays, Object Lock, access points, directory
buckets, other AWS partitions, S3-compatible services and endpoint overrides
remain outside this qualification.

The bounded source, binary, fixture, raw LIST and conditional-request evidence
is recorded in
[`linux-direct-amazon-s3.md`](linux-direct-amazon-s3.md).
