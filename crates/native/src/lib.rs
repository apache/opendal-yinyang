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

//! In-process bridge for native adapters. No daemon, socket or implicit host I/O.
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use yinyang::core::service::ServiceClient;
use yinyang::core::{Authority, BackendProfile, Fs, Node, NodeId, NodeKind, Revision, Snapshot};
use yinyang::mount::{Mount, MountFile};
use yinyang::runtime::{Error, ErrorKind, Result};
use yinyang::sync::{EditId, Sync};

mod ffi;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub storage: BTreeMap<String, String>,
    pub profile: String,
    pub staging: PathBuf,
    #[serde(default)]
    pub read_only: bool,
    pub service_address: Option<std::net::SocketAddr>,
    pub service_token: Option<String>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Mount,
    Sync,
}
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Root,
    Changes {
        revision: String,
        ordinal: u32,
        limit: usize,
    },
    Node {
        node: String,
        revision: Option<String>,
    },
    Lookup {
        parent: String,
        name: String,
    },
    Scan {
        node: String,
        revision: Option<String>,
        offset: usize,
        limit: usize,
    },
    Read {
        node: String,
        offset: u64,
        length: usize,
    },
    Write {
        node: String,
        offset: u64,
        bytes: Vec<u8>,
    },
    Truncate {
        node: String,
        length: u64,
    },
    Fsync,
    Release {
        node: String,
    },
    Refresh {
        node: String,
    },
    Create {
        parent: String,
        name: String,
        directory: bool,
    },
    Remove {
        node: String,
    },
    Rename {
        node: String,
        parent: String,
        name: String,
        replace: bool,
    },
    Download {
        node: String,
        revision: String,
        offset: u64,
        length: usize,
    },
    Stage {
        node: String,
        revision: String,
        path: PathBuf,
    },
    Publish {
        edit: String,
    },
    EditStatus {
        edit: String,
    },
}
pub struct Session {
    authority: Arc<dyn Authority>,
    mount: Option<Mount>,
    sync: Option<Sync>,
    files: BTreeMap<NodeId, MountFile>,
    read_only: bool,
}
impl Session {
    pub async fn connect(config: Config, mode: Mode) -> Result<Self> {
        let profile = match config.profile.as_str() {
            "minio" => BackendProfile::Minio,
            "amazon-s3" => BackendProfile::AmazonS3,
            _ => return Err(Error::Invalid("unsupported native backend profile")),
        };
        if !config.staging.is_absolute() {
            return Err(Error::Invalid("native staging must be absolute"));
        }
        opendal::install_default();
        let operator = opendal::Operator::via_iter("s3", config.storage)
            .map_err(|_| Error::State(ErrorKind::Storage, "invalid S3 operator configuration"))?;
        let authority: Arc<dyn Authority> = if let Some(address) = config.service_address {
            if !address.ip().is_loopback() {
                return Err(Error::Invalid("native service transport requires loopback"));
            }
            Arc::new(
                ServiceClient::connect(
                    address,
                    config
                        .service_token
                        .ok_or(Error::Invalid("service token is required"))?,
                    operator,
                )
                .await?,
            )
        } else {
            if config.service_token.is_some() {
                return Err(Error::Invalid("service token without service address"));
            }
            Arc::new(Fs::open(operator, profile).await?)
        };
        Self::open(authority, &config.staging, config.read_only, mode).await
    }
    pub async fn open(
        authority: Arc<dyn Authority>,
        staging: &std::path::Path,
        read_only: bool,
        mode: Mode,
    ) -> Result<Self> {
        let (mount, sync) = match mode {
            Mode::Mount => (
                Some(Mount::open(authority.clone(), staging, read_only).await?),
                None,
            ),
            Mode::Sync => {
                if read_only {
                    return Err(Error::Invalid(
                        "read-only Sync upload coordinator is unsupported",
                    ));
                }
                (None, Some(Sync::open(authority.clone(), staging).await?))
            }
        };
        Ok(Self {
            authority,
            mount,
            sync,
            files: BTreeMap::new(),
            read_only,
        })
    }
    fn mount(&self) -> Result<&Mount> {
        self.mount
            .as_ref()
            .ok_or(Error::Invalid("operation requires Mount mode"))
    }
    fn sync(&self) -> Result<&Sync> {
        self.sync
            .as_ref()
            .ok_or(Error::Invalid("operation requires Sync mode"))
    }
    async fn observation(&self, revision: Option<String>) -> Result<Snapshot> {
        match revision {
            Some(value) => Ok(self
                .authority
                .observe_revision(Revision::from_bytes(decode(&value)?))
                .await?),
            None => Ok(self.authority.observe_latest().await?),
        }
    }
    async fn file(&mut self, id: NodeId) -> Result<&MountFile> {
        if !self.files.contains_key(&id) {
            let file = self.mount()?.open_node(id, !self.read_only).await?;
            self.files.insert(id, file);
        }
        Ok(&self.files[&id])
    }
    async fn metadata(&self, snapshot: &Snapshot, node: Node) -> Result<Value> {
        let mut value = metadata(snapshot, &node);
        if let Some(file) = self.files.get(&node.id()) {
            value["size"] = json!(file.status().await?.length);
        }
        Ok(value)
    }
    pub async fn call(&mut self, request: Request) -> Result<Value> {
        match request {
            Request::Changes {
                revision,
                ordinal,
                limit,
            } => {
                if limit == 0 || limit > 256 {
                    return Err(Error::Invalid("invalid change page size"));
                }
                let revision = Revision::from_bytes(decode(&revision)?);
                self.authority.observe_revision(revision).await?;
                let snapshot = self.authority.observe_latest().await?;
                let records = snapshot
                    .changes(
                        Some(yinyang::core::object::Cursor { revision, ordinal }),
                        limit,
                    )
                    .await?;
                let more = records.len() == limit;
                let cursor = records
                    .last()
                    .map(|r| r.receipt.cursor)
                    .unwrap_or(yinyang::core::object::Cursor { revision, ordinal });
                let mut changes = Vec::new();
                for record in records {
                    let observation = self
                        .authority
                        .observe_revision(record.receipt.cursor.revision)
                        .await?;
                    for change in record.changes {
                        changes.push(json!({"before":change.before.map(|n|metadata(&observation,&n)),"after":change.after.map(|n|metadata(&observation,&n))}));
                    }
                }
                Ok(
                    json!({"changes":changes,"revision":encode(&cursor.revision.to_bytes()),"ordinal":cursor.ordinal,"more":more}),
                )
            }
            Request::Root => {
                let snapshot = self.authority.observe_latest().await?;
                let node = snapshot
                    .node(self.authority.root())
                    .await?
                    .ok_or(Error::Invalid("missing root"))?;
                self.metadata(&snapshot, node).await
            }
            Request::Node { node, revision } => {
                let snapshot = self.observation(revision).await?;
                let node = snapshot
                    .node(node_id(&node)?)
                    .await?
                    .ok_or(Error::State(ErrorKind::NotFound, "node not found"))?;
                self.metadata(&snapshot, node).await
            }
            Request::Lookup { parent, name } => {
                let snapshot = self.authority.observe_latest().await?;
                let entry = snapshot
                    .lookup(node_id(&parent)?, &name)
                    .await?
                    .ok_or(Error::State(ErrorKind::NotFound, "entry not found"))?;
                let node = snapshot
                    .node(entry.node_id)
                    .await?
                    .ok_or(Error::Invalid("entry without node"))?;
                self.metadata(&snapshot, node).await
            }
            Request::Scan {
                node,
                revision,
                offset,
                limit,
            } => {
                if limit == 0 || limit > 256 || offset > 1_000_000 {
                    return Err(Error::Invalid("invalid native page bounds"));
                }
                let snapshot = self.observation(revision).await?;
                let node = node_id(&node)?;
                let mut token = None;
                let mut skipped = 0;
                // The durable cursor is (revision, directory, ordinal). Rewalk
                // immutable pages after restart instead of persisting process IDs.
                while skipped < offset {
                    let page = snapshot
                        .scan(node, token.as_ref(), (offset - skipped).min(256))
                        .await?;
                    skipped += page.entries.len();
                    token = page.next;
                    if token.is_none() && skipped < offset {
                        return Err(Error::Invalid("directory cursor beyond end"));
                    }
                }
                if offset != 0 && token.is_none() {
                    return Ok(
                        json!({"revision": encode(&snapshot.revision().to_bytes()), "entries": [], "next": null}),
                    );
                }
                let page = snapshot.scan(node, token.as_ref(), limit).await?;
                let mut entries = Vec::new();
                for entry in &page.entries {
                    let node = snapshot
                        .node(entry.node_id)
                        .await?
                        .ok_or(Error::Invalid("entry without node"))?;
                    entries.push(self.metadata(&snapshot, node).await?);
                }
                Ok(
                    json!({"revision": encode(&snapshot.revision().to_bytes()), "entries": entries,
                    "next": page.next.map(|_| offset + page.entries.len())}),
                )
            }
            Request::Read {
                node,
                offset,
                length,
            } => {
                bounded(length)?;
                Ok(json!({"bytes": self.file(node_id(&node)?).await?.read(offset, length).await?}))
            }
            Request::Write {
                node,
                offset,
                bytes,
            } => {
                bounded(bytes.len())?;
                self.file(node_id(&node)?)
                    .await?
                    .write(offset, &bytes)
                    .await?;
                Ok(json!({"written": bytes.len()}))
            }
            Request::Truncate { node, length } => {
                self.file(node_id(&node)?).await?.truncate(length).await?;
                Ok(json!({}))
            }
            Request::Fsync => {
                self.mount()?.fsync().await?;
                Ok(json!({}))
            }
            Request::Release { node } => {
                let id = node_id(&node)?;
                self.files.remove(&id);
                match self.mount()?.reclaim(id).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::Busy => {}
                    Err(error) => return Err(error),
                }
                Ok(json!({}))
            }
            Request::Refresh { node } => {
                self.mount()?.refresh(node_id(&node)?).await?;
                Ok(json!({}))
            }
            Request::Create {
                parent,
                name,
                directory,
            } => {
                let mount = self.mount()?;
                let parent = node_id(&parent)?;
                if directory {
                    mount.namespace().create_directory(parent, &name).await?;
                } else {
                    mount.namespace().create_file(parent, &name).await?;
                }
                let snapshot = self.authority.observe_latest().await?;
                let entry = snapshot
                    .lookup(parent, &name)
                    .await?
                    .ok_or(Error::State(ErrorKind::NotFound, "created item moved"))?;
                let node = snapshot
                    .node(entry.node_id)
                    .await?
                    .ok_or(Error::State(ErrorKind::NotFound, "created item removed"))?;
                self.metadata(&snapshot, node).await
            }
            Request::Remove { node } => {
                self.mount()?.namespace().unlink(node_id(&node)?).await?;
                Ok(json!({}))
            }
            Request::Rename {
                node,
                parent,
                name,
                replace,
            } => {
                let (node, parent) = (node_id(&node)?, node_id(&parent)?);
                if replace {
                    self.mount()?
                        .namespace()
                        .rename_replace(node, parent, &name)
                        .await?;
                } else {
                    self.mount()?
                        .namespace()
                        .rename(node, parent, &name)
                        .await?;
                }
                Ok(json!({}))
            }
            Request::Download {
                node,
                revision,
                offset,
                length,
            } => {
                bounded(length)?;
                let snapshot = self.observation(Some(revision)).await?;
                let file = snapshot.open_file(node_id(&node)?).await?;
                Ok(json!({"bytes": file.read(offset, length).await?}))
            }
            Request::Stage {
                node,
                revision,
                path,
            } => {
                let mut file = tokio::fs::File::open(path)
                    .await
                    .map_err(|_| Error::State(ErrorKind::Io, "cannot open local edit"))?;
                let edit = self
                    .sync()?
                    .stage(
                        node_id(&node)?,
                        Revision::from_bytes(decode(&revision)?),
                        &mut file,
                    )
                    .await?;
                Ok(json!({"edit": encode(edit.as_bytes())}))
            }
            Request::Publish { edit } => {
                let revision = self
                    .sync()?
                    .publish(EditId::from_bytes(decode(&edit)?))
                    .await?;
                Ok(json!({"revision": encode(&revision.to_bytes())}))
            }
            Request::EditStatus { edit } => {
                let state = self
                    .sync()?
                    .status(EditId::from_bytes(decode(&edit)?))
                    .await?;
                Ok(
                    json!({"pending":state.pending,"frozen":state.frozen,"conflict":state.conflict,
                    "revision":encode(&state.remote_revision.to_bytes()),"size":state.length}),
                )
            }
        }
    }
}
fn metadata(snapshot: &Snapshot, node: &Node) -> Value {
    let size = match node.kind() {
        NodeKind::File(content) => content.content_id().length(),
        _ => 0,
    };
    json!({"node": encode(node.id().as_bytes()), "parent": node.link().map(|link|encode(link.parent.as_bytes())),
        "name": node.link().map(|link|link.name.as_str()).unwrap_or("YinYang"),
        "directory":node.is_directory(),"size":size,"executable":node.executable(),
        "generation":node.generation(),"revision":encode(&snapshot.revision().to_bytes())})
}
fn bounded(length: usize) -> Result<()> {
    if length > 1024 * 1024 {
        Err(Error::State(
            ErrorKind::TooLarge,
            "native I/O exceeds 1 MiB",
        ))
    } else {
        Ok(())
    }
}
fn encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn decode<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2 || !text.is_ascii() {
        return Err(Error::Invalid("invalid native identity"));
    }
    let mut bytes = [0; N];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid native identity"))?;
    }
    Ok(bytes)
}
fn node_id(text: &str) -> Result<NodeId> {
    Ok(NodeId::from_bytes(decode(text)?))
}
