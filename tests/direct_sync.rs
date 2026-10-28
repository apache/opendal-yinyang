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

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use yinyang::direct_sync::{
    BindingState, DirectRemote, DirectSync, EntryState, MutationOutcome, RemoteEntry, RemoteKind,
    RemotePin, RemotePrecondition, RemoteSnapshot, Resolution,
};

#[derive(Clone)]
struct MemoryRemote {
    namespace: String,
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    generation: Arc<AtomicU64>,
    unknown_next_publish: Arc<AtomicBool>,
    pause_next_publish: Arc<AtomicBool>,
    publish_reached: Arc<Notify>,
    publish_release: Arc<Notify>,
}

#[derive(Clone)]
struct RemoteHandle {
    remote: MemoryRemote,
}

impl MemoryRemote {
    fn new() -> (Self, RemoteHandle) {
        let remote = Self {
            namespace: "scheme=s3;profile=minio;bucket=test;root=/objects/;endpoint=loopback"
                .to_owned(),
            objects: Arc::new(Mutex::new(BTreeMap::new())),
            generation: Arc::new(AtomicU64::new(1)),
            unknown_next_publish: Arc::new(AtomicBool::new(false)),
            pause_next_publish: Arc::new(AtomicBool::new(false)),
            publish_reached: Arc::new(Notify::new()),
            publish_release: Arc::new(Notify::new()),
        };
        (remote.clone(), RemoteHandle { remote })
    }
}

impl RemoteHandle {
    fn put(&self, path: &str, bytes: &[u8]) {
        self.remote
            .objects
            .lock()
            .unwrap()
            .insert(path.to_owned(), bytes.to_vec());
    }

    fn delete(&self, path: &str) {
        self.remote.objects.lock().unwrap().remove(path);
    }

    fn read(&self, path: &str) -> Option<Vec<u8>> {
        self.remote.objects.lock().unwrap().get(path).cloned()
    }

    fn lose_next_publish_response(&self) {
        self.remote
            .unknown_next_publish
            .store(true, Ordering::SeqCst);
    }

    fn pause_next_publish(&self) {
        self.remote.pause_next_publish.store(true, Ordering::SeqCst);
    }

    async fn wait_until_published(&self) {
        self.remote.publish_reached.notified().await;
    }

    fn release_publish(&self) {
        self.remote.publish_release.notify_waiters();
    }
}

impl DirectRemote for MemoryRemote {
    fn namespace_id(&self) -> String {
        self.namespace.clone()
    }

    fn observe(&self) -> impl Future<Output = yinyang::replica::Result<RemoteSnapshot>> + Send {
        let objects = self.objects.clone();
        async move {
            let objects = objects.lock().unwrap();
            let mut entries = BTreeMap::new();
            for (path, bytes) in objects.iter() {
                entries.insert(
                    path.clone(),
                    RemoteEntry {
                        path: path.clone(),
                        kind: RemoteKind::File,
                        size: bytes.len() as u64,
                        pin: Some(pin(path, bytes)),
                        observation: observation(path, bytes),
                    },
                );
            }
            Ok(RemoteSnapshot {
                revision: Some(format!("generation-{}", observation_all(&objects))),
                entries,
            })
        }
    }

    fn read_to_path<'a>(
        &'a self,
        entry: &'a RemoteEntry,
        output: &'a Path,
    ) -> impl Future<Output = yinyang::replica::Result<()>> + Send + 'a {
        async move {
            let bytes = self
                .objects
                .lock()
                .unwrap()
                .get(&entry.path)
                .cloned()
                .ok_or_else(|| yinyang::replica::Error::conflict("remote object disappeared"))?;
            if observation(&entry.path, &bytes) != entry.observation {
                return Err(yinyang::replica::Error::conflict(
                    "remote object changed after observation",
                ));
            }
            std::fs::write(output, bytes)
                .map_err(|error| yinyang::replica::Error::io("write download", output, error))
        }
    }

    fn publish_file<'a>(
        &'a self,
        path: &'a str,
        source: &'a Path,
        precondition: &'a RemotePrecondition,
    ) -> impl Future<Output = yinyang::replica::Result<MutationOutcome>> + Send + 'a {
        async move {
            let mut bytes = Vec::new();
            std::fs::File::open(source)
                .and_then(|mut file| file.read_to_end(&mut bytes))
                .map_err(|error| yinyang::replica::Error::io("read upload", source, error))?;
            {
                let objects = self.objects.lock().unwrap();
                if !condition_matches(objects.get(path), precondition) {
                    return Ok(MutationOutcome::Conflict);
                }
            }
            self.objects
                .lock()
                .unwrap()
                .insert(path.to_owned(), bytes.clone());
            self.generation.fetch_add(1, Ordering::SeqCst);
            if self.pause_next_publish.swap(false, Ordering::SeqCst) {
                self.publish_reached.notify_waiters();
                self.publish_release.notified().await;
            }
            if self.unknown_next_publish.swap(false, Ordering::SeqCst) {
                return Ok(MutationOutcome::Unknown {
                    error: "test lost the successful response".to_owned(),
                });
            }
            Ok(MutationOutcome::Applied {
                accepted: Some(pin(path, &bytes)),
            })
        }
    }

    fn delete<'a>(
        &'a self,
        path: &'a str,
        precondition: &'a RemotePrecondition,
    ) -> impl Future<Output = yinyang::replica::Result<MutationOutcome>> + Send + 'a {
        async move {
            let mut objects = self.objects.lock().unwrap();
            if !condition_matches(objects.get(path), precondition) {
                return Ok(MutationOutcome::Conflict);
            }
            objects.remove(path);
            Ok(MutationOutcome::Applied { accepted: None })
        }
    }
}

fn condition_matches(current: Option<&Vec<u8>>, precondition: &RemotePrecondition) -> bool {
    match (current, precondition) {
        (None, RemotePrecondition::Absent) => true,
        (Some(_), RemotePrecondition::Absent) => false,
        (Some(bytes), RemotePrecondition::Etag(expected)) => {
            pin("", bytes).etag.as_deref() == Some(expected.as_str())
        }
        (None, RemotePrecondition::Etag(_)) => false,
    }
}

fn pin(path: &str, bytes: &[u8]) -> RemotePin {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(bytes);
    RemotePin {
        version: None,
        etag: Some(format!("\"{}\"", hasher.finalize().to_hex())),
        size: bytes.len() as u64,
        modified: None,
    }
}

fn observation(path: &str, bytes: &[u8]) -> [u8; 16] {
    let pin = pin(path, bytes);
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(pin.etag.unwrap().as_bytes());
    let mut observation = [0; 16];
    observation.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    observation
}

fn observation_all(objects: &BTreeMap<String, Vec<u8>>) -> String {
    let mut hasher = blake3::Hasher::new();
    for (path, bytes) in objects {
        hasher.update(path.as_bytes());
        hasher.update(bytes);
    }
    hasher.finalize().to_hex().to_string()
}

struct Fixture {
    temp: tempfile::TempDir,
    root: PathBuf,
    state: PathBuf,
    remote: MemoryRemote,
    handle: RemoteHandle,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        let state = temp.path().join("state");
        std::fs::create_dir(&root).unwrap();
        let (remote, handle) = MemoryRemote::new();
        Self {
            temp,
            root,
            state,
            remote,
            handle,
        }
    }

    async fn init(&self) -> DirectSync<MemoryRemote> {
        DirectSync::init(self.remote.clone(), &self.state, &self.root)
            .await
            .unwrap()
    }

    async fn open(&self) -> DirectSync<MemoryRemote> {
        DirectSync::open(self.remote.clone(), &self.state)
            .await
            .unwrap()
    }

    fn write(&self, path: &str, bytes: &[u8]) {
        let path = self.root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn read(&self, path: &str) -> Vec<u8> {
        std::fs::read(self.root.join(path)).unwrap()
    }
}

#[tokio::test]
async fn upload_pull_and_conditional_deletes_use_storage_native_state() {
    let fixture = Fixture::new();
    fixture.write("local.txt", b"local");
    let mut sync = fixture.init().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.published_local, 1);
    assert!(outcome.in_sync);
    assert_eq!(
        fixture.handle.read("local.txt").as_deref(),
        Some(b"local".as_slice())
    );
    drop(sync);

    fixture.handle.put("remote.txt", b"remote");
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.materialized_remote, 1);
    assert!(outcome.in_sync);
    assert_eq!(fixture.read("remote.txt"), b"remote");
    drop(sync);

    std::fs::remove_file(fixture.root.join("local.txt")).unwrap();
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.deleted_remote, 1);
    assert!(outcome.in_sync);
    assert!(fixture.handle.read("local.txt").is_none());
    drop(sync);

    fixture.handle.delete("remote.txt");
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.deleted_local, 1);
    assert!(outcome.in_sync);
    assert!(!fixture.root.join("remote.txt").exists());
}

#[tokio::test]
async fn external_and_local_replacement_conflict_retains_both_sides() {
    let fixture = Fixture::new();
    fixture.write("file", b"base");
    let mut sync = fixture.init().await;
    sync.run().await.unwrap();
    drop(sync);

    fixture.write("file", b"local");
    fixture.handle.put("file", b"remote");
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.conflicts, 1);
    assert!(!outcome.in_sync);
    assert_eq!(fixture.read("file"), b"local");
    assert_eq!(
        fixture.handle.read("file").as_deref(),
        Some(b"remote".as_slice())
    );

    let inspection =
        DirectSync::<MemoryRemote>::inspect(Some(&fixture.remote), &fixture.state, None).await;
    let entry = inspection
        .entries
        .iter()
        .find(|entry| entry.path == "file")
        .unwrap();
    assert_eq!(entry.state, EntryState::Conflict);
    let conflict = inspection.conflicts.first().unwrap();

    let mut local = Vec::new();
    sync.export_conflict_side(&conflict.id, "local", &mut local)
        .unwrap();
    assert_eq!(local, b"local");
    let mut remote = Vec::new();
    sync.export_conflict_side(&conflict.id, "remote", &mut remote)
        .unwrap();
    assert_eq!(remote, b"remote");
}

#[tokio::test]
async fn unknown_survives_restart_and_explicit_keep_local_observes_current_state() {
    let fixture = Fixture::new();
    fixture.write("file", b"v1");
    let mut sync = fixture.init().await;
    sync.run().await.unwrap();
    drop(sync);

    fixture.write("file", b"v2");
    fixture.handle.lose_next_publish_response();
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.unknown, 1);
    assert_eq!(
        fixture.handle.read("file").as_deref(),
        Some(b"v2".as_slice())
    );
    drop(sync);

    fixture.write("file", b"v3");
    let mut sync = fixture.open().await;
    let outcome = sync.run().await.unwrap();
    assert_eq!(outcome.unknown, 1);
    assert_eq!(fixture.read("file"), b"v3");
    assert_eq!(
        fixture.handle.read("file").as_deref(),
        Some(b"v2".as_slice())
    );

    let inspection =
        DirectSync::<MemoryRemote>::inspect(Some(&fixture.remote), &fixture.state, None).await;
    let work = inspection
        .entries
        .iter()
        .find(|entry| entry.path == "file")
        .and_then(|entry| entry.work.id.clone())
        .unwrap();
    sync.resolve(&work, Resolution::KeepLocal).await.unwrap();
    assert_eq!(
        fixture.handle.read("file").as_deref(),
        Some(b"v3".as_slice())
    );
    let inspection =
        DirectSync::<MemoryRemote>::inspect(Some(&fixture.remote), &fixture.state, None).await;
    assert!(inspection.summary.in_sync, "{inspection:?}");
}

#[tokio::test]
async fn accepted_older_input_does_not_acknowledge_a_newer_local_edit() {
    let fixture = Fixture::new();
    fixture.write("file", b"v1");
    let mut sync = fixture.init().await;
    sync.run().await.unwrap();
    drop(sync);

    fixture.write("file", b"v2");
    fixture.handle.pause_next_publish();
    let mut sync = fixture.open().await;
    let handle = fixture.handle.clone();
    let task = tokio::spawn(async move {
        let outcome = sync.run().await;
        (outcome, sync)
    });
    handle.wait_until_published().await;
    fixture.write("file", b"v3");
    handle.release_publish();
    let (outcome, sync) = task.await.unwrap();
    let outcome = outcome.unwrap();
    assert_eq!(outcome.published_local, 1);
    assert!(!outcome.in_sync);
    assert_eq!(outcome.pending, 1);
    assert_eq!(
        fixture.handle.read("file").as_deref(),
        Some(b"v2".as_slice())
    );
    drop(sync);

    let inspection =
        DirectSync::<MemoryRemote>::inspect(Some(&fixture.remote), &fixture.state, None).await;
    let entry = inspection
        .entries
        .iter()
        .find(|entry| entry.path == "file")
        .unwrap();
    assert_eq!(entry.state, EntryState::LocalPending);
    assert_eq!(fixture.read("file"), b"v3");
}

#[tokio::test]
async fn wrong_storage_binding_rejects_before_recovery_or_effects() {
    let fixture = Fixture::new();
    fixture.write("file", b"base");
    let mut sync = fixture.init().await;
    sync.run().await.unwrap();
    drop(sync);

    let before = fixture.handle.read("file");
    let mut wrong = fixture.remote.clone();
    wrong.namespace =
        "scheme=s3;profile=minio;bucket=other;root=/objects/;endpoint=loopback".to_owned();
    let error = DirectSync::open(wrong.clone(), &fixture.state)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("bound to namespace"));
    assert_eq!(fixture.handle.read("file"), before);

    let inspection = DirectSync::<MemoryRemote>::inspect(Some(&wrong), &fixture.state, None).await;
    assert_eq!(inspection.binding.state, BindingState::Error);
}
