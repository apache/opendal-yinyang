# Linux application workflows

Status: experimental Ubuntu 24.04 x86_64 application trials through writable
Managed `fuse.yinyang` mounts and a quiesced read-only backup view. These are
narrow command shapes, not a general POSIX filesystem support claim.

Use the directly downloadable `yy` from the
[trial distribution guide](linux-trial-distribution.md). The application
workflow adds no installer, downloader, activation framework or consumer
toolchain requirement.

## Tested matrix

The fresh-main probe and continuous acceptance use the Ubuntu 24.04 packages
listed here. Another version or build is not covered until it runs through the
same acceptance entry point.

| Application | Tested workflow | Result | Recommendation |
| --- | --- | --- | --- |
| GNU tar 1.35 | `--no-same-owner --touch --no-overwrite-dir` extraction of representable files; archive creation; symlink rejection | Pass | Use the explicit flags below and validate the extracted payload |
| rsync 3.2.7 | `--inplace` update of an existing representable destination tree, including `--delete` | Pass | Use for in-place updates only; do not assume default safe-save or new-file creation |
| Git 2.43.0 | `git init --template=`; `hash-object -w` against a representable bare skeleton | Fail with `EOPNOTSUPP` on private temporary/loose-object modes | Not supported under the current mode contract |

Both passing workflows ran against object and authenticated loopback service
publication. The test checked bytes, executable mode, namespace, independent
authority visibility and a clean unmount/remount.

## Prerequisites

The native acceptance requires Linux, `/dev/fuse`, `fusermount3`, GNU tar,
rsync and git:

```shell
sudo apt-get update
sudo apt-get install --yes fuse3 git rsync tar
```

Set the same isolated `YINYANG_S3_*` and `YINYANG_STORAGE_PROFILE=minio`
environment used by the FUSE acceptance. Missing configuration, mount support
or application binaries are hard failures.

## rsync

The tested update command is:

```shell
rsync -r --checksum --no-times --no-owner --no-group --omit-dir-times \
  --inplace --delete SOURCE/ /path/to/mount/DESTINATION/
```

The destination directories and files must already exist with representable
modes before the first update, including an empty placeholder for each newly
added source file. The acceptance verifies replacement bytes, an executable
file, an updated new source entry, removal of a source-deleted file and the
same tree after remount.

Default rsync safe-save and new-file creation use mode `0600`, which the mount
rejects with `EOPNOTSUPP`; the acceptance asserts that failure and that no
destination entry is left behind. `--inplace` does not make new-file creation
succeed. This guide does not claim general rsync mirroring, metadata
preservation, hard links, symlinks, ACLs or xattrs.

## GNU tar

Extract into an existing mounted directory:

```shell
tar -xf archive.tar -C /path/to/mount/RESTORE \
  --no-same-owner --touch --no-overwrite-dir
```

Stop writes, unmount cleanly and remount the same volume read-only before
creating the backup archive:

```shell
tar -cf /var/tmp/mounted-tree.tar -C /path/to/read-only/mount/RESTORE .
```

The archive path must be outside the mount and GNU tar must exit 0. The
read-only view reports `0444` files and `0555` executable files/directories,
so the archive preserves payload bytes and the executable distinction but not
the earlier writable bits. Extract the complete archive locally and compare
every expected payload and executable bit.

An archive containing a symlink must fail extraction with a symlink diagnostic
and must not substitute another file type. The mount still does not support
links.

## Git boundary

The Git probe ran:

```shell
git init --template= repository
git --git-dir /path/to/mount/repository.git hash-object -w source-file
```

The first command needs a private `.git` temporary file and fails with
`EOPNOTSUPP`; the second uses a representable bare-repository skeleton and
fails at the loose-object temporary file before an object exists. Both report
`Unable to create temporary file ... Operation not supported`. Git's private
temporary and loose-object modes are not representable by the current
`0644`/`0755` file contract. Git workflows are excluded rather than silently
using a different mode model.

## Run

```shell
cargo x test-linux-applications
```

The entry point runs `tests/linux_applications.rs`, requires real
`fuse.yinyang`, and uses `YY_UPGRADE_BIN` when set. The trial-distribution
workflow sets that selector to the packaged archive's `bin/yy`, so application
acceptance can be tied to the same binary offered to consumers.
