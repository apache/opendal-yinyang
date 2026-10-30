# 546f419 stage-3 compatibility fixture

This directory is immutable cross-version evidence. Do not regenerate or
edit these files with the current checkout.

The payloads were created by the release `yy` binary from
`546f419f9158e156f9a561822f3cb3f307222584`. The producer archive SHA-256 and
packaged binary SHA-256 are recorded in `manifest.json`. The producer tree
equals `ba7c7f2c3bbebd16c01891bc2361d9d36217d503`.

The fixture contains stage-3 sparse bases and segments, a pending handle, a
request dropped before commit, a successful commit whose response was lost,
and a service metadata database copied together with its non-empty WAL.
`expected.json` records exact handles, CommitIds, request digests, segment
ranges, payload hashes and the service WAL identity.

`manifest.json` hashes every other file with SHA-256. `.gitattributes` keeps
the fixture bytes exact on every checkout platform.
