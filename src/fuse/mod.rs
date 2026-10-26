// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Linux FUSE mount frontend for Managed read-only volumes.
//!
//! The CLI form is `yy --volume CONFIG mount MOUNTPOINT`; the process stays in
//! the foreground until unmounted. The volume must declare `frontend = "fuse"`
//! and `read_only = true`; the shared admission rules in
//! [`crate::volume`] reject every other combination before an authority
//! connection or staging lease is created.
//!
//! Startup takes the staging lease through [`crate::mount::Mount`] and then
//! refuses to serve a staging database that still holds unpublished, frozen,
//! conflicted or failed handles: a read-only snapshot view must never surface
//! those locally staged bytes as if they were remote. Clean observations are
//! disposable and are reclaimed by the existing Mount recovery.
//!
//! The session mounts as `fuse.yinyang` and runs in the foreground. SIGINT and
//! SIGTERM detach the mount and wait briefly for the session. Lazy unmounting
//! does not end the kernel connection while clients still hold open
//! references, so the entry point returns [`Outcome::TeardownExpired`] when a
//! bounded wait expires. The CLI handles that outcome by exiting non-zero,
//! which closes `/dev/fuse` and makes the kernel abort every pending request.
//!
//! See `specs/linux-fuse.md` for the full read-only contract.

mod attrs;
mod frontend;
mod inode;

use crate::mount::Mount;
use crate::volume::{Volume, VolumeConfig};
use fuser::{Config, MountOption, Session, SessionACL};
use std::path::PathBuf;
use std::sync::Arc;

/// How long a signal-initiated teardown waits after a lazy unmount before it
/// reports that the caller must terminate the process.
const TEARDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Result of a foreground mount run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// The session ended on its own or a signal-initiated unmount finished.
    Unmounted,
    /// A lazy unmount left the kernel connection alive past the grace period
    /// because clients still hold references. The caller must terminate the
    /// process so the OS closes `/dev/fuse` and the kernel aborts pending
    /// requests (equivalent to crash recovery, not a graceful unmount).
    TeardownExpired { grace: std::time::Duration },
}

/// Mount a validated read-only FUSE volume in the foreground.
///
/// `staging` must already be resolved to an absolute path by the CLI.
pub async fn run(
    config: VolumeConfig,
    operator: opendal::Operator,
    token: Option<String>,
    mountpoint: PathBuf,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let authority = Volume::connect(&config, operator, token).await?;
    let mount = Mount::open(authority, &config.staging, true).await?;
    reject_unpublished_staging(&mount).await?;
    let snapshot = mount.authority().observe_latest().await?;
    let root = mount.authority().root();
    let root_node = snapshot.node(root).await?;
    if !root_node.is_some_and(|node| node.is_directory()) {
        return Err(std::io::Error::other(
            "remote filesystem root is unavailable or not a directory",
        )
        .into());
    }
    let inner = Arc::new(frontend::Inner::new(
        mount,
        tokio::runtime::Handle::current(),
    ));
    let filesystem = frontend::FuseFrontend {
        inner: inner.clone(),
    };
    let mut options = Config::default();
    options.mount_options = vec![
        MountOption::RO,
        MountOption::FSName(format!("yinyang-{}", config.name)),
        MountOption::Subtype("yinyang".into()),
    ];
    options.acl = SessionACL::Owner;
    let session = Session::new(filesystem, &mountpoint, &options)?;
    println!(
        "yy: mounted {} read-only at {} (fuse.yinyang)",
        config.name,
        mountpoint.display()
    );
    // `Session::spawn` moves the mount object into `BackgroundSession`, so a
    // `SessionUnmounter` obtained from the pre-spawn session would find an
    // empty slot. One blocking owner holds the background session, polls its
    // public thread guard for a natural end (for example `fusermount3 -u`),
    // and receives the local teardown request. Exactly one `join` or
    // `umount_and_join` runs.
    let background = session.spawn()?;
    inner.install_notifier(background.notifier());
    let refresh = inner.clone();
    tokio::runtime::Handle::current().spawn(async move { refresh.serve_hup().await });
    let (teardown, teardown_rx) = std::sync::mpsc::channel();
    let join = tokio::task::spawn_blocking(move || {
        loop {
            match teardown_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return background.umount_and_join();
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if background.guard.is_finished() {
                        return background.join();
                    }
                }
            }
        }
    });
    tokio::pin!(join);
    let outcome = tokio::select! {
        result = &mut join => {
            // The session ended on its own, for example through an external
            // `fusermount3 -u`. Returning now lets the process exit and release
            // the staging lease instead of waiting for a signal forever.
            result.map_err(blocking_error)??;
            Outcome::Unmounted
        }
        _ = shutdown_signal() => {
            let _ = teardown.send(());
            match tokio::time::timeout(TEARDOWN_GRACE, &mut join).await {
                Ok(result) => {
                    result.map_err(blocking_error)??;
                    Outcome::Unmounted
                }
                // A lazy unmount keeps the session alive while clients hold
                // the mount. Report it to the CLI instead of returning into a
                // runtime shutdown that would wait for the blocked session
                // task; the CLI exits the process so the kernel aborts the
                // connection.
                Err(_) => Outcome::TeardownExpired {
                    grace: TEARDOWN_GRACE,
                },
            }
        }
    };
    if matches!(outcome, Outcome::Unmounted) {
        println!("yy: unmounted {}", mountpoint.display());
    }
    Ok(outcome)
}

fn blocking_error(error: tokio::task::JoinError) -> std::io::Error {
    std::io::Error::other(format!("filesystem session task failed: {error}"))
}

/// A read-only snapshot must never expose staged bytes that were not
/// published. The staging lease is already held, so the check cannot race
/// another client, and refusing startup changes no remote state.
async fn reject_unpublished_staging(mount: &Mount) -> Result<(), Box<dyn std::error::Error>> {
    let status = mount.status().await?;
    if let Some(handle) = status
        .handles
        .iter()
        .find(|handle| handle.pending || handle.frozen || handle.conflict || handle.error.is_some())
    {
        return Err(std::io::Error::other(format!(
            "staging contains handle {} with unpublished, frozen, conflicted or failed state; \
             a read-only FUSE mount will not expose it; publish or acknowledge it with the \
             library frontend before mounting",
            handle.id
        ))
        .into());
    }
    Ok(())
}

/// Wait for SIGINT or SIGTERM, the two signals a foreground mount operator uses
/// to tear the session down cleanly.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::reject_unpublished_staging;
    use crate::core::{BackendProfile, Fs};
    use crate::mount::Mount;
    use crate::runtime::Runtime;
    use crate::support;
    use std::sync::Arc;
    use yinyang_core::Authority;

    #[tokio::test]
    async fn read_only_mount_refuses_unpublished_staging() {
        let temp = tempfile::tempdir().unwrap();
        let backend = support::TestBackend::default();
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::open(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let staging = temp.path().join("staging");
        let runtime = Runtime::open(authority.clone(), &staging).await.unwrap();
        runtime
            .create_file(runtime.authority().root(), "dirty")
            .await
            .unwrap();
        let mut handle = runtime.open_file("dirty", true).await.unwrap();
        handle.write(0, b"unpublished").await.unwrap();
        drop(handle);
        drop(runtime);
        let mount = Mount::open(authority, &staging, true).await.unwrap();
        let error = reject_unpublished_staging(&mount).await.unwrap_err();
        assert!(error.to_string().contains("unpublished"));
    }

    #[tokio::test]
    async fn read_only_mount_accepts_clean_staging() {
        let temp = tempfile::tempdir().unwrap();
        let backend = support::TestBackend::default();
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap();
        let authority: Arc<dyn Authority> = Arc::new(
            Fs::open(backend.operator(), BackendProfile::Minio)
                .await
                .unwrap(),
        );
        let mount = Mount::open(authority, temp.path().join("staging"), true)
            .await
            .unwrap();
        reject_unpublished_staging(&mount).await.unwrap();
    }
}
