# Linux application trials

**Status: experimental Ubuntu 24.04 x86_64 validation of two application
workflows.**
This specification defines the tested rsync and GNU tar command shapes, their
file-type and mode boundaries, and the observed Git exclusion. It does not add
general POSIX application compatibility, metadata preservation, locking, link
support or a new consumer installation path.

## Common mount boundary

Each workflow uses a writable Managed volume through
`yy --volume CONFIG mount MOUNTPOINT` and the real `fuse.yinyang` mount type.
The application receives the same synthesized namespace as other mount users:

- files are `0644` or executable `0755`;
- directories are fixed at `0755`;
- ownership and timestamps are synthesized and are not preserved;
- symlinks, hard links and special nodes are unsupported.

The acceptance covers object and authenticated loopback service publication.
After application success it checks bytes and modes through the mount and an
independent authority, unmounts cleanly, remounts the same staging directory
and repeats the namespace checks. A backup archive is created only after writes
have stopped and the same volume has been remounted read-only.

## rsync existing-tree update

The tested command is:

```text
rsync -r --checksum --no-times --no-owner --no-group --omit-dir-times \
  --inplace [--delete] SOURCE/ DESTINATION/
```

`DESTINATION` must already contain the directories and files named by
`SOURCE`. Entries covered by the workflow are regular files with `0644` or
`0755` modes and directories fixed at `0755`. The workflow verifies byte
replacement, an executable file, a new source entry, deletion of a stale
destination entry, and the same result after remount.

This is an explicit in-place update contract. New destination files must be
pre-created as part of the setup. rsync's default safe-save temporary files and
its new-file creation path request mode `0600`, which the Managed mount rejects
with `EOPNOTSUPP`; acceptance asserts that failure and that no destination
entry is left behind. Pre-creating the destination tree is test setup for the
supported update workflow, not a general rsync workaround. The specification
makes no claim about rsync copy-on-write, hard-link, timestamp, ownership, ACL,
xattr or link-preserving modes.

## GNU tar extraction and archive

The tested restore command is:

```text
tar -xf ARCHIVE -C DESTINATION \
  --no-same-owner --touch --no-overwrite-dir
```

The flags avoid ownership and timestamp restoration. Archive entries must use
the mount's representable file types and modes. The workflow restores files and
directories into a writable mount and checks payload bytes and executable mode.
`--no-overwrite-dir` preserves the pre-created `0755` extraction root; the
acceptance asserts that root separately from payload and child modes.
It then stops writing, unmounts cleanly and remounts the same staging directory
read-only before creating the backup archive with:

```text
tar -cf ARCHIVE -C MOUNTPOINT .
```

`ARCHIVE` must be outside the mounted tree. The archive command must exit 0.
The read-only mount reports files as `0444` and executable files and
directories as `0555`, so the backup archive records those normalized modes:
payload bytes and the executable distinction survive, while the earlier
writable `0644`/`0755` bits and synthesized timestamps are not a POSIX
preservation claim. Acceptance extracts the complete archive and compares
every expected payload and executable bit.

An archive containing a symlink is a required negative case. Extraction returns
a nonzero status whose stderr names the symlink operation, and the destination
must not contain a substituted regular file or directory at that path.

## Git observation

The fresh-main probe ran Git 2.43.0 in a writable Managed mount. This command
failed before repository initialization completed:

```text
git init --template= repository
```

Git attempted to create a private temporary file below `.git`; the mount
returned `EOPNOTSUPP`, and Git exited 128 with
`Unable to create temporary file ... Operation not supported`. A second
regression prepares a bare repository skeleton with representable modes and
runs `git hash-object -w`; Git's loose-object temporary file also requests a
non-representable mode, so the command fails with the same diagnostic before
an object exists. Git's private temporary and loose-object modes are outside
the current representable mode contract. Git is therefore excluded from the
supported application matrix; the trial does not hide this failure or broaden
the product mode model.

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
