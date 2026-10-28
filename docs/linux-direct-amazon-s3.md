# Amazon S3 Direct qualification

**Status: bounded qualification record for 2026-10-28.** The default endpoint,
two bespoke buckets, temporary credentials and one Linux host are the tested
configuration. This record does not claim another region, account, bucket
policy, access point, directory bucket, version-suspended bucket or
S3-compatible service.

## Qualified matrix

| Field | Qualified value |
| --- | --- |
| Backend | Amazon S3 general-purpose buckets |
| Region and endpoint | `us-east-2`, provider default endpoint with no override |
| Versioning | one bucket never enabled; one bucket `Status=Enabled` |
| Host | Ubuntu 24.04.5 LTS x86_64, kernel `7.0.0-1014-aws` |
| Frontend | real `/dev/fuse` and `fusermount3`, `fuse.yinyang` |
| Rust | `rustc 1.99.1`, `cargo 1.99.1` |
| OpenDAL | 0.59.5 |
| Raw evidence client | official AWS CLI 2.39.0 |
| Measured source | `ddd70b626f6201c86795a269ad8ab88dfed97a2e` |

The measured source contains the OpenDAL 0.59.5 dependency foundation needed
to evaluate conditional Direct mutation. Commits after the measured source
that only add this qualification record do not change the executable.

The debug `yy` binary under test had SHA-256
`05d87bcbbf4b945879351590cf68a2b1887bb218e6fd482a634e3b1f37c1f587`.
The `linux_direct` test executable had SHA-256
`d1fde0b6d86f7d030f69b26e7741bf12f6f85a2cee1b94a0cd08a75ecc1f5b02`.
These are native acceptance binary identities, not release artifacts.

## Fixtures and authority

The qualification created only these task-owned resources in account
`054483968661`:

| Resource | Name or ARN |
| --- | --- |
| Never-versioned bucket | `yinyang-direct-q-35b90fdf-plain` |
| Versioned bucket | `yinyang-direct-q-35b90fdf-versioned` |
| Temporary role | `arn:aws:iam::054483968661:role/yinyang-direct-qualification-35b90fdf` |

Both buckets enable all four public-access blocks and default SSE-S3
encryption. The role trusts only
`arn:aws:iam::054483968661:user/xuanwo`, has a maximum session duration of
7200 seconds and has one inline policy scoped to those two buckets. The policy
allows bucket/location/version inventory and object/version read, put, delete,
abort and multipart-list operations. It cannot change versioning, bucket
policies, public-access settings or encryption. No instance profile, user
policy or existing shared bucket was changed.

The Q run used 3600-second STS credentials delivered as JSON over a FIFO to
the sanitizing test wrapper. Credentials were not placed in command arguments,
files, tmux buffers or logs. P/O may create fresh 7200-second sessions from the
same role immediately before acquiring a runner slot. Buckets and the role
remain available until all consumers finish; deleting them is a coordinator
decision.

## Executed acceptance

The command on the qualified host was:

```text
bash /home/ubuntu/Code/opendal-yinyang/experiments/linux-devbox/run-validation.sh \
  q-real-s3-ddd70b6-r5 python3 tests/linux_direct_real_s3.py
```

The shared runner reported exit 0. The test executable reported:

```text
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out;
finished in 154.74s
```

The seven tests covered:

- 1005-object recursive LIST through 28 non-aggregated ListObjectsV2 pages,
  exact GET, complete FUSE enumeration and byte comparison at the first,
  middle and last keys;
- a 1002-object adversarial namespace with `a` as the first key and `a/b` as
  the last key, exact GET for both, later-page collision discovery and actual
  mount rejection;
- percentage escaping, DEL, spaces, case distinctions and NFC/NFD names on
  real Amazon S3, including SIGTERM teardown, remount and unchanged inventory;
- true version IDs, replacement, ListObjectVersions retention, delete marker,
  old-descriptor bytes and attributes, SIGHUP and fresh opens;
- never-versioned ETag replacement and deletion through FUSE, where an old
  descriptor retained its attributes but reads returned stale errors and a
  fresh SIGHUP observation exposed the replacement or absence;
- unversioned and versioned conditional create, replace and delete primitives,
  concurrent races, stale delete protection and missing-key behavior.

### Complete LIST evidence

| Case | Keys | Page sizes | Key-set BLAKE3 |
| --- | ---: | --- | --- |
| Normal recursive namespace | 1005 | 27 pages of 37 and 1 page of 6 | `916471959ecf0b68c615879d2a1934595a56821dd75d99c5cd6acf8d27d1cc76` |
| Later-page collision | 1002 | 27 pages of 37 and 1 page of 3 | `02d16d2a8cb66f1aaf04e034be7baf9fd0a747bd9fe93f5a1abcdc58ddcadfc0` |
| Reversible-key namespace | 9 | 1 page of 9 | `bb44b5e3db07ac029c5559dc1cc30d66e5a8f92b8e317254254fa0ccaefc1e0b` |

The AWS CLI calls included `--no-paginate`; the test requested each
`--max-keys 37` page and followed `NextContinuationToken` itself. The actual
service returned exactly 37 keys for each non-final page. The collision
namespace's first key was `a`, its last key was `a/b`, and raw exact GET
returned both objects before the CLI/FUSE mount rejected the complete
observation.

The nine reversible keys were:

```text
plain.txt
literal%.txt
literal%25.txt
control<DEL>
space name.txt
Case.txt
case.txt
café
cafe<combining acute accent>
```

All nine exact GETs and FUSE reads returned their seeded bytes. The two percent
spellings remained distinct, case remained significant and NFC/NFD were not
normalized.

## Conditional request findings

These are observations from raw AWS CLI requests through the same default
Amazon S3 endpoint. They qualify backend primitives that a writable Direct
runtime must use; they do not claim that writable Direct is implemented.

| Request | Observed result |
| --- | --- |
| `PUT If-None-Match: *` on a missing key | creates the object |
| `PUT If-None-Match: *` on an existing key | `412 PreconditionFailed`; existing bytes remain |
| `PUT If-Match: <noncurrent ETag>` | `412 PreconditionFailed`; current bytes remain |
| two concurrent `PUT If-Match: <same current ETag>` | exactly one succeeds; the loser observed either `409 ConditionalRequestConflict` or `412 PreconditionFailed` across runs |
| two concurrent `PUT If-None-Match: *` creates | exactly one succeeds; the loser observed `412 PreconditionFailed` |
| `DELETE If-Match: <stale ETag>` | `412 PreconditionFailed`; replacement bytes remain |
| `DELETE If-Match: <current ETag>` | succeeds and removes the current object |
| conditional DELETE after physical deletion, using exact or `*` | `404 NoSuchKey` |
| conditional DELETE after a versioned delete marker, using exact or `*` | `404 NoSuchKey`; old versions remain readable |

Versioned `PUT If-Match` creates a new current version. Versioned conditional
DELETE creates a delete marker and retains both prior versions. A conditional
DELETE on a delete marker does not create another marker: Amazon S3 reports
the current key as missing with `404 NoSuchKey`.

The runtime must treat both 409 and 412 as uncommitted conditional outcomes.
A 409 is transient contention and requires a fresh observation before retry;
it does not establish that the attempted bytes were published. The suite did
not simulate a lost response after a committed request. A writer therefore
also needs restart/re-observation evidence for that uncertainty boundary
rather than inferring commit from a disconnected client.

The relevant Amazon S3 contracts are:

- [PutObject `If-Match`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfMatch)
- [PutObject `If-None-Match`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfNoneMatch)
- [DeleteObject `If-Match`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html#AmazonS3-DeleteObject-request-header-IfMatch)
- [Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Conditional deletes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)

## Evidence

The raw runner log is retained outside the repository at:

```text
/Users/xuanwo/Code/experiments/yinyang/linux-next-2026-10-28/Q-real-s3-ddd70b6-r5.log
```

It contains the source SHA, exact tests, observed request outcomes, page
sizes, key-set digests, pass count and timestamps. Its SHA-256 is
`06f4b873173dff70088c5afd6a1620fcd2d613cc560e02dbee4f06ba7932ba88`.
The retained fixture manifest, role trust and inline-policy documents are
siblings with filenames beginning `Q-fixture-manifest`, `Q-aws-role-trust` and
`Q-aws-role-policy`.

After the suite, both buckets had zero current keys and zero retained
version/delete-marker entries. No `yy` process or `fuse.yinyang` mount remained.
The versioned bucket still reported `Status=Enabled`; the unversioned bucket's
GetBucketVersioning response remained empty. EC2 and the unrelated Silo
fixture remain running.

## Limits

- The suite uses debug native acceptance binaries rather than an installed
  release artifact.
- Q exercised raw conditional mutation primitives only. Writable Direct Mount,
  Direct Sync, lost-response recovery and retention after an uncertain write
  remain subject to their own runtime acceptance.
- Pagination is not an atomic snapshot. External writes during a multi-page
  observation remain outside the tested concurrency guarantee.
- The suite qualifies one us-east-2 configuration. It does not generalize to
  other AWS partitions, regions, credentials, bucket policies, Requester Pays,
  Object Lock, access points, directory buckets or version-suspended buckets.
- The concurrent races observed 409 and 412 outcomes, but repeated randomized
  contention, network failure injection and long-running soak are not part of
  this record.
