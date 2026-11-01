# Linux application workflows

Status: experimental Ubuntu 24.04 x86_64 application trials through writable
Managed `fuse.yinyang` mounts and a quiesced read-only backup view. These are
narrow command shapes, not a general POSIX filesystem support claim.

Use the directly downloadable `yy` from the
[trial distribution guide](linux-trial-distribution.md) with an existing
configured Managed volume. The consumer workflow does not require Rust, Cargo,
mise, Git, a local storage fixture or a separate installer.

## Tested workflows

The tested Ubuntu 24.04 application packages are GNU tar 1.35 and rsync 3.2.7.
Another version or build is not covered until it runs through the command shape
below.

| Application | Tested workflow | Recommendation |
| --- | --- | --- |
| GNU tar 1.35 | Explicit extraction, a quiesced read-only backup archive, and symlink rejection | Use the flags below and validate the extracted payload |
| rsync 3.2.7 | `--inplace` update of an existing representable destination tree, including `--delete` | Use for in-place updates only |

Start a foreground mount with the configured volume:

```shell
yy --volume /absolute/path/to/volume.json mount /path/to/mountpoint
```

Keep `yy` running while the application operates on the mountpoint. See
[Linux operations](linux-operations.md) for volume setup and service choices.

## rsync update

The tested update command is:

```shell
rsync -r --checksum --no-times --no-owner --no-group --omit-dir-times \
  --inplace --delete SOURCE/ /path/to/mount/DESTINATION/
```

The destination directories and files must already exist with representable
modes before the first update, including an empty placeholder for each newly
added source file. The acceptance verifies replacement bytes, a new source
entry, removal of a source-deleted entry, executable mode and the same tree
after remount.

Default rsync safe-save and new-file creation use mode `0600`, which the mount
rejects with `EOPNOTSUPP`; no destination entry is left behind. `--inplace`
does not make new-file creation succeed. This guide does not claim general
rsync mirroring, metadata preservation, hard links, symlinks, ACLs or xattrs.

## GNU tar restore and backup

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

`--no-overwrite-dir` intentionally keeps the pre-existing extraction root at
its current mode. The acceptance treats that root as setup state and separately
checks every payload, child directory and executable mode.

An archive containing a symlink must fail extraction with a symlink diagnostic
and must not substitute another file type. The mount still does not support
links.

## Maintainer verification

This section does not describe a consumer requirement. It covers the repository
acceptance that produced the results above.

```shell
cargo x test-linux-applications
```

The entry point requires Linux, `/dev/fuse`, `fusermount3`, Git, rsync, GNU tar
and the isolated `YINYANG_S3_*`/`YINYANG_STORAGE_PROFILE=minio` fixture. Missing
tools or configuration are hard failures. The test runs both object and
authenticated loopback service publication and uses `YY_UPGRADE_BIN` when set.
The trial-distribution workflow sets that selector to the packaged archive's
`bin/yy`.

The maintainer suite also records the Git negative boundary.
`git init --template=` and `git hash-object -w` against a representable
bare-repository skeleton both fail with `EOPNOTSUPP` while creating private
temporary or loose-object files. Git workflows are excluded rather than
silently using a different mode model.
