# Linux trial distribution contract

**Status: experimental Ubuntu 24.04 x86_64 trial workflow.**
This specification defines one traceable local archive, verification,
versioned-prefix installation, stopped activation and non-destructive
retention-reporting boundary. It does not define an official release, signing,
ASF vote, tag, external publication or physical reclamation.

## Artifact identity

The archive name is:

```text
yinyang-<cargo-version>-ubuntu-24.04-x86_64-<12-character-source-sha>.tar.gz
```

`BUILD-MANIFEST` schema `yinyang-linux-release-artifact.v1` contains exactly:

- `schema`, `package`, `version`, `release`, `source_commit`;
- `source_date_epoch`, `target`, `yy_sha256`;
- `cargo` and `rustc` identity strings.

`release` is `<version>+<12-character-source-sha>`. `target` is
`ubuntu-24.04-x86_64`. The executable hash identifies the packaged `bin/yy`.
The source commit identifies the clean checkout used to build it; the archive
does not cryptographically prove that provenance.

The archive inventory is fixed by schema v1:

- `BUILD-MANIFEST`, `INSTALL`, `LICENSE`, `NOTICE`, `PREREQUISITES`;
- `bin/yy`;
- the three systemd user-unit templates;
- the environment example;
- the operational, qualification, recovery, compatibility, operating-envelope
  and trial-distribution documents;
- `install-release.sh` and `verify-release.sh`.

Every entry must be a regular file or directory. Absolute paths, `..`
components, backslashes, duplicate entries, symlinks, links to other entries and
entries outside the single archive root are rejected. New artifact content
requires an explicit specification and verifier update.

`LICENSE` and `NOTICE` must be present. `PREREQUISITES` schema
`yinyang-linux-release-prerequisites.v1` binds the same release, source and
target and states consumer packages and unsupported build-time requirements.

## Verification

`verify-release.sh` checks:

1. the adjacent SHA-256 sidecar names the archive and matches its bytes;
2. the exact safe inventory and manifest field set;
3. archive name, release, version, source and target consistency;
4. the packaged `bin/yy` SHA-256;
5. execution of `bin/yy --help`;
6. LICENSE, NOTICE and executable packaged scripts.

The optional expected source commit must equal the manifest source. The
checksum sidecar is an integrity input, not an authenticity or signature claim.

## Installation and activation

`install-release.sh install` verifies the archive again and creates exactly one
immutable directory:

```text
<prefix>/<cargo-version>+<12-character-source-sha>/
```

It refuses to replace an existing directory. Staging extraction and the final
release directory share the prefix filesystem. Installation does not modify the
current symlink unless `--activate` is explicit.

Activation requires the explicit `--quiesced` assertion that no `yy` writer uses
the prefix. It validates the release's executable and, when present, its
manifest release and binary SHA-256. It atomically replaces `current`. It does
not create an automatic rollback pointer, pending operation or state monitor.

The installer never invokes `systemctl`, starts a mount or service, modifies
configuration or state, or infers compatibility from equal package versions.

## Transition boundary

The installer has no rollback or downgrade command. After a newer binary has
opened or written configuration, staging, Sync, journal, receipt or service
state, launching an older binary against that state is unsupported. The
operator must use compatibility admission and a quiesced backup or isolated
restore instead.

The supported cross-version evidence remains an exact producer/target pair.
`linux_prefix_upgrade` qualifies `fd1338c` against the selected target archive
binary while preserving pending, frozen, Unknown, receipt and service-WAL
state. Equal package versions do not create broader compatibility.

## Read-only accounting and retention

`install-release.sh inspect` reads the versioned prefix and reports:

- `current`;
- every direct release directory and its `du -sb` size;
- the retained release count;
- accounting scope `installed-release-directories-only`.

The report does not inspect or classify backend objects, retained versions,
delete markers, non-atomic listings, staging, metadata, WAL, SHM, receipts,
pending/frozen/Unknown work, external readers, shared physical packs, backups or
archives. Missing evidence remains unknown rather than zero or reclaimable.

No command in this contract deletes a release, state, archive or backend object.
Physical reclamation, retention expiry and automatic garbage collection are
outside this milestone. A known local size or backend byte total does not by
itself authorize deletion.

## Acceptance

The fresh-runner workflow must:

- build from a clean checkout and retain the archive, sidecar, manifest, source
  and binary SHA-256;
- reject a changed archive checksum and an attempted immutable-directory
  overwrite;
- install the actual archive and perform only explicit quiesced activation;
- exercise installed diagnostics and recovery through the real packaged binary,
  using the shared native-acceptance binary selector and recording its SHA-256;
- exercise the existing installed-operations foreground writable Mount, remount
  and finite Sync entrypoint with the same packaged binary and selector;
- build the pinned legacy producer and run the existing installed-prefix
  upgrade with the archive's target binary;
- retain pending, frozen, Unknown, receipts, Sync and service-WAL evidence;
- upload these files only as normal short-lived Actions evidence.

Hosted CI verifies the packaged unit files but does not create a systemd user
manager or claim the explicit qualified-host systemd lifecycle. The workflow
does not sign, tag or publish the artifact.
