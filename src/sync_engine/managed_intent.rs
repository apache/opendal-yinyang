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

use super::adapter::{FrozenRemoteRequest, RemoteOperation, RemotePin, RemoteRequest};
use super::{Error, Result};
use borsh::BorshSerialize;
use std::sync::Arc;
use yinyang_core::{Authority, CommitId, NodeId, Planner, Revision, Transaction};

#[derive(BorshSerialize)]
enum RequestKey {
    CreateFile {
        parent: [u8; 16],
        parent_revision: [u8; 24],
        parent_digest: Option<[u8; 32]>,
        parent_executable: bool,
        parent_directory: bool,
        name: String,
        executable: bool,
        length: u64,
        digest: [u8; 32],
    },
    ReplaceFile {
        node: [u8; 16],
        baseline_revision: [u8; 24],
        baseline_digest: Option<[u8; 32]>,
        baseline_executable: bool,
        baseline_directory: bool,
        length: u64,
        digest: [u8; 32],
    },
    CreateDirectory {
        parent: [u8; 16],
        parent_revision: [u8; 24],
        parent_digest: Option<[u8; 32]>,
        parent_executable: bool,
        parent_directory: bool,
        name: String,
    },
    Remove {
        node: [u8; 16],
        baseline_revision: [u8; 24],
        baseline_digest: Option<[u8; 32]>,
        baseline_executable: bool,
        baseline_directory: bool,
    },
    Rename {
        node: [u8; 16],
        baseline_revision: [u8; 24],
        baseline_digest: Option<[u8; 32]>,
        baseline_executable: bool,
        baseline_directory: bool,
        parent: [u8; 16],
        parent_revision: [u8; 24],
        parent_digest: Option<[u8; 32]>,
        parent_executable: bool,
        parent_directory: bool,
        name: String,
        replace: bool,
    },
    SetExecutable {
        node: [u8; 16],
        baseline_revision: [u8; 24],
        baseline_digest: Option<[u8; 32]>,
        baseline_executable: bool,
        baseline_directory: bool,
        executable: bool,
    },
}

pub(super) async fn freeze(
    authority: &Arc<dyn Authority>,
    request: RemoteRequest,
) -> Result<FrozenRemoteRequest> {
    let operation = request.operation();
    let path = request.path().to_owned();
    let original_path = request.original_path().map(str::to_owned);
    let desired_path = request.desired_path().map(str::to_owned);
    let transaction = match request {
        RemoteRequest::CreateFile {
            path,
            source,
            executable,
            parent,
        } => {
            let parent = parent.ok_or_else(|| Error::corrupt("Managed create has no parent"))?;
            let RemotePin::Managed {
                node: parent,
                revision,
                digest,
                executable: parent_executable,
                directory: parent_directory,
            } = parent.pin
            else {
                return Err(Error::corrupt("Managed create received a Direct parent"));
            };
            let mut file = tokio::fs::File::open(&source)
                .await
                .map_err(|error| Error::io("open Managed upload", &source, error))?;
            let prepared = authority.prepare(&mut file).await.map_err(Error::from)?;
            let content = prepared.descriptor().content_id();
            let key = RequestKey::CreateFile {
                parent,
                parent_revision: revision,
                parent_digest: digest,
                parent_executable,
                parent_directory,
                name: name(&path).to_owned(),
                executable,
                length: content.length(),
                digest: *content.digest(),
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            planner
                .create_file(
                    NodeId::from_bytes(parent),
                    name(&path),
                    prepared,
                    executable,
                )
                .await
                .map_err(Error::from)?;
            planner.finish().map_err(Error::from)?
        }
        RemoteRequest::ReplaceFile {
            source, baseline, ..
        } => {
            let RemotePin::Managed {
                node,
                revision,
                digest,
                executable,
                directory,
            } = baseline.pin
            else {
                return Err(Error::corrupt("Managed replace received a Direct baseline"));
            };
            let mut file = tokio::fs::File::open(&source)
                .await
                .map_err(|error| Error::io("open Managed upload", &source, error))?;
            let prepared = authority.prepare(&mut file).await.map_err(Error::from)?;
            let content = prepared.descriptor().content_id();
            let key = RequestKey::ReplaceFile {
                node,
                baseline_revision: revision,
                baseline_digest: digest,
                baseline_executable: executable,
                baseline_directory: directory,
                length: content.length(),
                digest: *content.digest(),
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            let _ = planner.node(NodeId::from_bytes(node)).await?;
            planner
                .set_content(NodeId::from_bytes(node), prepared)
                .await
                .map_err(Error::from)?;
            planner.finish().map_err(Error::from)?
        }
        RemoteRequest::CreateDirectory { path, parent } => {
            let RemotePin::Managed {
                node: parent,
                revision,
                digest,
                executable: parent_executable,
                directory: parent_directory,
            } = parent.pin
            else {
                return Err(Error::corrupt(
                    "Managed directory create received a Direct parent",
                ));
            };
            let key = RequestKey::CreateDirectory {
                parent,
                parent_revision: revision,
                parent_digest: digest,
                parent_executable,
                parent_directory,
                name: name(&path).to_owned(),
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            planner
                .create_directory(NodeId::from_bytes(parent), name(&path))
                .await
                .map_err(Error::from)?;
            planner.finish().map_err(Error::from)?
        }
        RemoteRequest::Remove { baseline, .. } => {
            let RemotePin::Managed {
                node,
                revision,
                digest,
                executable,
                directory,
            } = baseline.pin
            else {
                return Err(Error::corrupt("Managed remove received a Direct baseline"));
            };
            let key = RequestKey::Remove {
                node,
                baseline_revision: revision,
                baseline_digest: digest,
                baseline_executable: executable,
                baseline_directory: directory,
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            let _ = planner.node(NodeId::from_bytes(node)).await?;
            planner
                .remove(NodeId::from_bytes(node))
                .await
                .map_err(Error::from)?;
            planner.finish().map_err(Error::from)?
        }
        RemoteRequest::Rename {
            to,
            baseline,
            new_parent,
            replace,
            ..
        } => {
            let RemotePin::Managed {
                node,
                revision,
                digest,
                executable,
                directory,
            } = baseline.pin
            else {
                return Err(Error::corrupt("Managed rename received a Direct baseline"));
            };
            let RemotePin::Managed {
                node: new_parent,
                revision: parent_revision,
                digest: parent_digest,
                executable: parent_executable,
                directory: parent_directory,
            } = new_parent.pin
            else {
                return Err(Error::corrupt("Managed rename received a Direct parent"));
            };
            let key = RequestKey::Rename {
                node,
                baseline_revision: revision,
                baseline_digest: digest,
                baseline_executable: executable,
                baseline_directory: directory,
                parent: new_parent,
                parent_revision,
                parent_digest,
                parent_executable,
                parent_directory,
                name: name(&to).to_owned(),
                replace,
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            let _ = planner.node(NodeId::from_bytes(node)).await?;
            let destination = planner
                .lookup(NodeId::from_bytes(new_parent), name(&to))
                .await?;
            if let Some(replaced) = destination
                .as_ref()
                .filter(|entry| entry.node_id != NodeId::from_bytes(node))
            {
                let _ = planner.node(replaced.node_id).await?;
            }
            if replace {
                planner
                    .rename_replace(
                        NodeId::from_bytes(node),
                        NodeId::from_bytes(new_parent),
                        name(&to),
                    )
                    .await
                    .map_err(Error::from)?;
            } else {
                planner
                    .rename(
                        NodeId::from_bytes(node),
                        NodeId::from_bytes(new_parent),
                        name(&to),
                    )
                    .await
                    .map_err(Error::from)?;
            }
            planner.finish().map_err(Error::from)?
        }
        RemoteRequest::SetExecutable {
            baseline,
            executable,
            ..
        } => {
            let RemotePin::Managed {
                node,
                revision,
                digest,
                executable: current_executable,
                directory,
            } = baseline.pin
            else {
                return Err(Error::corrupt(
                    "Managed metadata edit received a Direct baseline",
                ));
            };
            let key = RequestKey::SetExecutable {
                node,
                baseline_revision: revision,
                baseline_digest: digest,
                baseline_executable: current_executable,
                baseline_directory: directory,
                executable,
            };
            let (_, commit_id) = identity(&key)?;
            let snapshot = authority
                .observe_revision(Revision::from_bytes(revision))
                .await
                .map_err(Error::from)?;
            let mut planner = Planner::new(&snapshot, commit_id);
            let _ = planner.node(NodeId::from_bytes(node)).await?;
            planner
                .set_executable(NodeId::from_bytes(node), executable)
                .await
                .map_err(Error::from)?;
            planner.finish().map_err(Error::from)?
        }
    };
    frozen(transaction, operation, path, original_path, desired_path)
}

pub(super) fn restore(
    transaction: Transaction,
    request: &FrozenRemoteRequest,
) -> Result<Transaction> {
    if transaction.id().as_bytes() != &request.id || transaction.digest() != request.digest {
        return Err(Error::corrupt(
            "restored Managed transaction identity disagrees with the frozen request",
        ));
    }
    Ok(transaction)
}

fn frozen(
    transaction: Transaction,
    operation: RemoteOperation,
    path: String,
    original_path: Option<String>,
    desired_path: Option<String>,
) -> Result<FrozenRemoteRequest> {
    Ok(FrozenRemoteRequest {
        id: *transaction.id().as_bytes(),
        digest: transaction.digest(),
        bytes: transaction.to_bytes()?,
        operation,
        path,
        original_path,
        desired_path,
    })
}

fn identity(key: &RequestKey) -> Result<([u8; 16], CommitId)> {
    let key = borsh::to_vec(key).map_err(|error| Error::corrupt(error.to_string()))?;
    let request = blake3::derive_key("yinyang-sync-engine-managed-request-1", &key);
    let commit = blake3::derive_key("yinyang-sync-engine-managed-commit-1", &request);
    Ok((
        request[..16].try_into().expect("fixed request prefix"),
        CommitId::from_bytes(commit[..16].try_into().expect("fixed commit prefix")),
    ))
}

fn name(path: &str) -> &str {
    path.rsplit_once('/').map_or(path, |(_, name)| name)
}

pub(super) async fn prepare(
    authority: &Arc<dyn Authority>,
    request: &FrozenRemoteRequest,
) -> Result<Transaction> {
    let transaction = authority
        .restore_transaction(&request.bytes)
        .await
        .map_err(Error::from)?;
    restore(transaction, request)
}
