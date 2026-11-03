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
| rsync 3.2.7 | Creation of an absent destination tree and default safe-save replacement, including `--delete` | Use the command below and validate bytes and modes |

Start a foreground mount with the configured volume:

```shell
yy --volume /absolute/path/to/volume.json mount /path/to/mountpoint
```

Keep `yy` running while the application operates on the mountpoint. See
[Linux operations](linux-operations.md) for volume setup and service choices.

## rsync creation and replacement

The tested command is:

```shell
rsync -r --checksum --no-times --no-owner --no-group --omit-dir-times \
  --delete SOURCE/ /path/to/mount/DESTINATION/
```

The destination may be absent. rsync creates it and its nested directories,
creates files from the source modes (including `0600` and `0755`), and replaces
existing files without `--inplace` or pre-created placeholders. The acceptance
checks byte replacement, private and executable modes, a new source entry,
removal of a source-deleted entry, and the same result after remount. Omit
`--delete` when source-deleted destination entries should be retained; the
acceptance also runs the same command without it.

rsync's safe-save temporary files use `O_CREAT|O_EXCL` and mode `0600`; the
mount persists that temporary mode, then rsync chmods the final mode before a
plain rename replaces the destination. A separate controlled case verifies the
same sequence with an already-open old descriptor: the old descriptor keeps
the old bytes, a fresh open sees the replacement, and a remount retains the
final mode and bytes for both Managed authorities. This guide does not claim
general rsync mirroring, ownership or timestamp preservation, hard links,
symlinks, ACLs or xattrs.

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
read-only view clears every write bit: `0644` becomes `0444`, `0755` files and
directories become `0555`, `0600` becomes `0400`, and `0700` becomes `0500`.
Extract the complete archive locally and compare every expected payload and
ordinary mode. The archive does not preserve ownership or timestamps.

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
and the isolated `YINYANG_S3_*` fixture configured as MinIO. Missing tools or
configuration are hard failures. The test runs both object and authenticated
loopback service publication and uses `YY_UPGRADE_BIN` when set. The
trial-distribution workflow sets that selector to the packaged archive's
`bin/yy`.

The suite prepares a bare repository whose files and directories use ordinary
modes, then runs `git hash-object -w` against it. The narrow probe verifies
that Git's private loose-object temporary reaches the Managed mode boundary,
the object is created, and its bytes can be read back through the mount. This
does not qualify repository updates, pack/index/locking operations or other Git
workflows.
