# nier validation runner (task-only)

This branch is transfer tooling for the 2026-11 architecture remediation. It is
not part of any product pull request.

Source reaches nier only through Git. Commit on the Mac, push the branch to the
`nier` remote (`nier:/home/xuanwo/Code/yinyang/dev.git`), then start a run with
the full commit SHA:

```shell
git push nier HEAD:refs/heads/xuanwo/<topic>
ssh nier ~/Code/yinyang/devbox/experiments/yy-nier/start.sh <label> xuanwo/<topic> <full-sha> \
  ~/Code/yinyang/devbox/experiments/yy-nier/suite.sh quick
ssh nier ~/Code/yinyang/devbox/experiments/yy-nier/wait.sh <label> 1500
ssh nier ~/Code/yinyang/devbox/experiments/yy-nier/status.sh <label> 80
```

- Labels are unique, lowercase `[a-z0-9-]`; reuse is rejected so every log is
  fresh evidence. Logs: `~/.local/state/yinyang-nier/runs/<label>.{log,exit}`.
- Each branch gets one worktree under `~/Code/yinyang/wt/`; runs on the same
  branch serialize. At most three runs execute at once.
- The environment uses the mbx Cargo shim, cargo-deny 0.14.22, and the local
  Silo at `127.0.0.1:19000` with buckets `yinyang-test` and the versioned
  `yinyang-direct-versioned`.
- `suite.sh quick` runs lint, check and test. `suite.sh full` adds the lockfile
  check, operating-envelope fault test, licenses, MSRV, S3, Managed Sync, Linux
  FUSE, Direct and operations acceptance. Any explicit step list also works,
  for example `suite.sh check linux-fuse`, or pass a direct command such as
  `cargo test -p yinyang --test sync some_filter`.
- `cargo x test-linux-recovery` needs passwordless sudo and the `sqlite3` CLI,
  which nier lacks; recovery acceptance is covered by GitHub CI only.
