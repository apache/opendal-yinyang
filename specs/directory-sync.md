# Managed directory reconciliation

**Status: experimental.**

Managed ordinary-directory synchronization is one `SyncEngine` profile from
[linux-sync.md](linux-sync.md). The engine supplies the local scan, common
baseline, reconciliation, artifact lifecycle, local installation journal,
conflicts, status and backup. This specification defines only the Managed
adapter contract.

## Remote identity

Managed remote entries have a stable 16-byte `NodeId`. The accepted baseline
records that node, its kind, executable bit, content identity and the exact
revision that accepted it. A later observation at the same path does not
replace the accepted revision.

The engine's local side of a baseline is the content accepted by a receipt. It
is never copied from the live local file after a response arrives. If a local
edit is newer than the accepted publication, it remains pending. If a retained
or live local side no longer equals the baseline local digest, the path is not
`in_sync`.

Managed content identity is verified by the core content descriptor. A download
writes into the engine-owned fd and verifies the expected content before the
engine installs it. Publication reuses the exact prepared request rather than
materializing fresh bytes at publish time.

## Observation

The adapter supports the `delta` capability. A successful observation returns
`ChangeRecord` values:

```text
ChangeRecord {
    receipt,
    changes: [
        Change {
            before: Option<Node>,
            after: Option<Node>,
        },
    ],
}
```

Changes are keyed by NodeId, not path. The engine applies them to the accepted
entry map and recomputes affected paths from stable node identity. A rename
whose new path has descendants re-walks only the new subtree from the returned
snapshot. A cursor that is no longer retained, a truncated page, or an adapter
that cannot supply a delta triggers exactly one full walk.

The cursor is persisted only in `baseline_meta.cursor`. No baseline, work or
binding row duplicates it.

## Publication

Managed reports `directories`, `executable`, `rename`, `batch`, `delta` and
`receipt_lookup`. `max_batch` is 4096 independent requests. The engine submits
one batch per directory depth for independent creates. Every batch member keeps
its own frozen request, CommitId, receipt and typed outcome.

The Managed adapter plans every namespace request internally against its
supplied baseline. It persists the resulting `Transaction` as the frozen
adapter request and cannot replace the common engine baseline or acknowledge
bytes different from the accepted input. Content edits prepare the local file
once and carry the resulting descriptor in the same per-pass batch.

Managed reports `Applied`, `Conflict`, `Unknown`, `Retryable` or `Unsupported`
for each request. A receipt identifies the exact request identity and accepted
revision and is not inferred from a later head observation.

## Change and outcome handling

Before dispatch, the engine commits the request as `Unknown`. Managed reports
`receipt_lookup`, so a retry first asks for the original receipt. Receipt lookup
by an exact request identity may accept that request without a second
publication. The accepted local side is the request input. A newer live local
file remains pending and is never folded into the completed baseline.

A local install is one engine-owned operation. Managed provides a callback that
writes the verified remote content into the supplied borrowed fd. The engine
owns the temporary file, race rechecks, durable `installs` row, rename or
exchange, baseline transition and artifact release.

## Conflict resolution

Managed conflict material follows [linux-sync.md](linux-sync.md). `--keep
remote` materializes the recorded node/revision without issuing a publication.
`--keep local` and `--content PATH` create a new request against a newly
observed baseline. The selected local bytes are frozen before dispatch and
remain the accepted local side after success.

## State and backup

Managed uses `yinyang-sync-engine-2` and the shared tables documented in
[linux-sync.md](linux-sync.md). The engine `intents` table is the only durable
Managed namespace/content intent log; the content-only `yinyang::sync::Sync`
profile is used only by the in-process native content bridge.

Object-mode Managed backup contains `sync-engine.db`, the bound root, retained
artifacts and the package manifest. Restore validates the package, rebinds local
identities and preserves engine intents, baselines and artifact content
evidence.
