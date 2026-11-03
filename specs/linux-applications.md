# Linux application trials

**Status: experimental Ubuntu 24.04 x86_64 validation of two application
workflows.**
This specification defines the tested rsync and GNU tar command shapes, their
file-type and ordinary-mode boundaries, and one narrow Git loose-object probe.
It does not add general POSIX application compatibility, ownership/timestamp
preservation, locking, link support or a new consumer installation path.

## Common mount boundary

Each workflow uses a writable Managed volume through
`yy --volume CONFIG mount MOUNTPOINT` and the real `fuse.yinyang` mount type.
The application receives the same synthesized namespace as other mount users:

- files and directories expose the ordinary modes persisted by Managed;
- common fixtures use `0644`/`0755` files and `0755` directories, while the
  rsync workflow also verifies private `0600` files and `0700` directories;
- ownership and timestamps are synthesized and are not preserved;
- symlinks, hard links and special nodes are unsupported.

The acceptance covers object and authenticated loopback service publication.
After application success it checks bytes and modes through the mount and an
independent authority, unmounts cleanly, remounts the same staging directory
and repeats the namespace checks. A backup archive is created only after writes
have stopped and the same volume has been remounted read-only.

## rsync creation and replacement

The tested command is:

```text
rsync -r --checksum --no-times --no-owner --no-group --omit-dir-times \
  [--delete] SOURCE/ DESTINATION/
```

`DESTINATION` is absent initially. rsync creates it and its nested directories,
creates files whose source modes include private `0600` and executable `0755`,
and replaces an existing destination on a later run. No target file is
pre-created and `--inplace` is not used.

The tested rsync creates safe-save temporaries with `O_CREAT|O_EXCL` and mode
`0600`, writes, applies the final mode with `fchmod`/`fchmodat2`, then replaces
the destination with plain `rename`. The Managed mount persists the requested
ordinary modes, so writes, chmod and atomic replacement share the existing
conditional publication path. Acceptance checks byte replacement, `0600`,
`0644`, `0700`, `0755`, a new source entry, deletion of a stale destination
entry, and the same result after remount.

The specification makes no claim about rsync hard-link, timestamp, ownership,
ACL, xattr or link-preserving modes.

A separate controlled safe-save case opens a destination before replacement,
creates a temporary with mode `0600`, writes and fsyncs new bytes, changes the
temporary to `0644`, and renames it over the destination. The old descriptor
must retain the old bytes, a fresh open must see the new bytes, and a clean
remount must retain the new bytes and mode. Both object and authenticated
loopback service publication are covered. The case uses an in-process
independent authority for the remote read; it does not spawn a CLI while a
dirty descriptor is intentionally held.

## GNU tar extraction and archive

The tested restore command is:

```text
tar -xf ARCHIVE -C DESTINATION \
  --no-same-owner --touch --no-overwrite-dir
```

The flags avoid ownership and timestamp restoration. Archive entries must use
supported file types and ordinary modes. The workflow restores files and
directories into a writable mount and checks payload bytes and modes.
`--no-overwrite-dir` preserves the pre-created `0755` extraction root; the
acceptance asserts that root separately from payload and child modes.
It then stops writing, unmounts cleanly and remounts the same staging directory
read-only before creating the backup archive with:

```text
tar -cf ARCHIVE -C MOUNTPOINT .
```

`ARCHIVE` must be outside the mounted tree. The archive command must exit 0.
The read-only mount clears every write bit. Common `0644` files therefore
become `0444`, `0755` files and directories become `0555`, `0600` becomes
`0400`, and `0700` becomes `0500`. The backup archive records that read-only
projection; synthesized timestamps remain outside the preservation claim.
Acceptance extracts the archive and compares expected payloads and modes.

An archive containing a symlink is a required negative case. Extraction returns
a nonzero status whose stderr names the symlink operation, and the destination
must not contain a substituted regular file or directory at that path.

## Git loose-object probe

The tested Git 2.43.0 probe prepares a bare repository whose files and
directories already use ordinary modes, then runs:

```text
git --git-dir REPOSITORY.git hash-object -w BLOB
```

The probe verifies that Git's `0600` temporary loose object reaches the Managed
ordinary-mode boundary and that the resulting object can be read back through
the mount. It is not a general Git workflow qualification; repository update,
pack, index, locking and other Git operations remain untested by this entry.

## Acceptance

Run:

```text
cargo x test-linux-applications
```

The command fails rather than skips when Linux, `/dev/fuse`, `fusermount3`,
`git`, `rsync`, GNU tar or the isolated S3 environment is missing. The harness
uses the shared `YY_UPGRADE_BIN` selector when present and otherwise runs the
Cargo test `yy` binary. Acceptance tools and versions describe only the tested
Ubuntu 24.04 application packages; another application build or backend remains
untested until it runs through the same entry point.
