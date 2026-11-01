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

//! Versioned, bounded RPC for a trusted local service deployment.
//! The initial transport is loopback-only; remote exposure requires an
//! authenticated encrypted transport and is deliberately rejected here.

use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_FRAME: usize = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(60);

#[derive(BorshSerialize, BorshDeserialize)]
struct Request {
    version: u8,
    token: String,
    command: Command,
}
#[derive(BorshSerialize, BorshDeserialize)]
enum Command {
    Observe(Option<[u8; 24]>),
    Get([u8; 24], u8, Vec<u8>),
    Scan([u8; 24], u8, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, u32),
    Register(Vec<u8>),
    Commit(Vec<Vec<u8>>),
    PrepareOverwrite([u8; 24], [u8; 16], Vec<(u64, u64, Vec<u8>)>),
    PrepareUpdate([u8; 24], [u8; 16], u64, u64, Vec<(u64, u64, Vec<u8>)>),
    History(Option<[u8; 24]>, u32),
    Expire(Vec<u8>),
    HistoryStates(Option<[u8; 24]>, u32),
}
#[derive(BorshSerialize, BorshDeserialize)]
enum Response {
    Observation([u8; 16], [u8; 16], [u8; 24]),
    Value(Option<Vec<u8>>),
    Rows(Rows),
    Registered,
    Outcomes(Vec<(u8, Vec<u8>)>),
    Error(u8, String),
    Prepared(Vec<u8>),
    Revisions(Vec<[u8; 24]>, Option<[u8; 24]>),
    Expiry(Vec<u8>),
    RevisionStates(Vec<([u8; 24], u8, Vec<u8>)>, Option<[u8; 24]>),
    Expired([u8; 24], String),
}
fn io(e: std::io::Error) -> Error {
    Error::from_io("metadata RPC", e)
}
fn protocol() -> Error {
    Error::invalid("metadata RPC", "unexpected protocol response")
}
fn kind_tag(kind: ErrorKind) -> u8 {
    match kind {
        ErrorKind::Invalid => 0,
        ErrorKind::Corrupt => 1,
        ErrorKind::NotFound => 2,
        ErrorKind::AlreadyExists => 3,
        ErrorKind::Unsupported => 4,
        ErrorKind::Storage => 5,
        ErrorKind::Io => 6,
        ErrorKind::InvalidName => 7,
        ErrorKind::NotDirectory => 8,
        ErrorKind::IsDirectory => 9,
        ErrorKind::NotEmpty => 10,
        ErrorKind::PermissionDenied => 11,
        ErrorKind::Expired => 12,
    }
}
fn tag_kind(tag: u8) -> Result<ErrorKind> {
    Ok(match tag {
        0 => ErrorKind::Invalid,
        1 => ErrorKind::Corrupt,
        2 => ErrorKind::NotFound,
        3 => ErrorKind::AlreadyExists,
        4 => ErrorKind::Unsupported,
        5 => ErrorKind::Storage,
        6 => ErrorKind::Io,
        7 => ErrorKind::InvalidName,
        8 => ErrorKind::NotDirectory,
        9 => ErrorKind::IsDirectory,
        10 => ErrorKind::NotEmpty,
        11 => ErrorKind::PermissionDenied,
        12 => ErrorKind::Expired,
        _ => return Err(protocol()),
    })
}

#[cfg(test)]
mod error_tests {
    use super::*;
    #[test]
    fn domain_error_tags_round_trip_without_changing_existing_tags() {
        let kinds = [
            ErrorKind::Invalid,
            ErrorKind::Corrupt,
            ErrorKind::NotFound,
            ErrorKind::AlreadyExists,
            ErrorKind::Unsupported,
            ErrorKind::Storage,
            ErrorKind::Io,
            ErrorKind::InvalidName,
            ErrorKind::NotDirectory,
            ErrorKind::IsDirectory,
            ErrorKind::NotEmpty,
            ErrorKind::PermissionDenied,
            ErrorKind::Expired,
        ];
        for (tag, kind) in kinds.into_iter().enumerate() {
            assert_eq!(kind_tag(kind), tag as u8);
            assert_eq!(tag_kind(tag as u8).unwrap(), kind);
        }
        assert!(tag_kind(255).is_err());
    }

    #[test]
    fn appended_overwrite_variants_preserve_existing_wire_tags() {
        assert_eq!(borsh::to_vec(&Command::Register(vec![])).unwrap()[0], 3);
        assert_eq!(borsh::to_vec(&Command::Commit(vec![])).unwrap()[0], 4);
        assert_eq!(
            borsh::to_vec(&Command::PrepareOverwrite([0; 24], [0; 16], vec![])).unwrap()[0],
            5
        );
        assert_eq!(
            borsh::to_vec(&Command::PrepareUpdate([0; 24], [0; 16], 0, 0, vec![])).unwrap()[0],
            6
        );
        assert_eq!(borsh::to_vec(&Command::History(None, 1)).unwrap()[0], 7);
        assert_eq!(borsh::to_vec(&Command::Expire(vec![])).unwrap()[0], 8);
        assert_eq!(
            borsh::to_vec(&Command::HistoryStates(None, 1)).unwrap()[0],
            9
        );
        assert_eq!(
            borsh::to_vec(&Response::Error(0, String::new())).unwrap()[0],
            5
        );
        assert_eq!(borsh::to_vec(&Response::Prepared(vec![])).unwrap()[0], 6);
        assert_eq!(
            borsh::to_vec(&Response::Revisions(vec![], None)).unwrap()[0],
            7
        );
        assert_eq!(borsh::to_vec(&Response::Expiry(vec![])).unwrap()[0], 8);
        assert_eq!(
            borsh::to_vec(&Response::RevisionStates(vec![], None)).unwrap()[0],
            9
        );
        assert_eq!(
            borsh::to_vec(&Response::Expired([0; 24], String::new())).unwrap()[0],
            10
        );
    }
}
async fn read_frame<T: BorshDeserialize>(stream: &mut TcpStream) -> Result<T> {
    let length = stream.read_u32().await.map_err(io)? as usize;
    if length > MAX_FRAME {
        return Err(Error::unsupported("metadata RPC", "frame exceeds 16 MiB"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await.map_err(io)?;
    decode(&bytes)
}
async fn write_frame(stream: &mut TcpStream, value: &impl BorshSerialize) -> Result<()> {
    let bytes = encode(value)?;
    if bytes.len() > MAX_FRAME {
        return Err(Error::unsupported("metadata RPC", "frame exceeds 16 MiB"));
    }
    stream.write_u32(bytes.len() as u32).await.map_err(io)?;
    stream.write_all(&bytes).await.map_err(io)?;
    stream.flush().await.map_err(io)
}
fn validate_endpoint(addr: SocketAddr, token: &str) -> Result<()> {
    if !addr.ip().is_loopback() || token.len() < 32 || token.len() > 1024 {
        return Err(Error::unsupported(
            "metadata RPC",
            "requires loopback and a 32..=1024 byte authentication token",
        ));
    }
    Ok(())
}
fn table(tag: u8) -> Result<Table> {
    Ok(match tag {
        0 => Table::Nodes,
        1 => Table::Entries,
        2 => Table::Receipts,
        3 => Table::Changes,
        _ => return Err(protocol()),
    })
}
fn token_matches(candidate: &str, expected: &str) -> bool {
    blake3::hash(candidate.as_bytes()) == blake3::hash(expected.as_bytes())
}

/// Serve authenticated local clients. No object checkpoint can accept writes
/// while this authority is unavailable. Stop the listener to stop accepting;
/// in-flight requests may still complete, so callers resolve their commit IDs.
pub async fn serve(listener: TcpListener, service: MetadataService, token: String) -> Result<()> {
    validate_endpoint(listener.local_addr().map_err(io)?, &token)?;
    let limit = Arc::new(tokio::sync::Semaphore::new(32));
    loop {
        let permit = limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| protocol())?;
        let (mut stream, _) = listener.accept().await.map_err(io)?;
        let service = service.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // A timeout drops the connection, never claims to roll back a commit.
            let _ = tokio::time::timeout(TIMEOUT, async {
                let response = match read_frame::<Request>(&mut stream).await {
                    Ok(request) if token_matches(&request.token, &token) => {
                        if request.version == crate::compatibility::RPC_PROTOCOL_VERSION {
                            dispatch(&service, request.command).await
                        } else {
                            Err(Error::unsupported(
                                "metadata RPC",
                                "unknown protocol version",
                            ))
                        }
                    }
                    _ => Err(Error::invalid(
                        "metadata RPC",
                        "invalid protocol or authentication",
                    )),
                };
                let response = match response {
                    Ok(response) => response,
                    Err(error)
                        if error.kind() == ErrorKind::Expired
                            && error.expired_revision().is_some() =>
                    {
                        Response::Expired(
                            error
                                .expired_revision()
                                .expect("checked expired identity")
                                .to_bytes(),
                            error.message().to_owned(),
                        )
                    }
                    Err(error) => {
                        Response::Error(kind_tag(error.kind()), error.message().to_owned())
                    }
                };
                write_frame(&mut stream, &response).await
            })
            .await;
        });
    }
}
async fn dispatch(service: &MetadataService, command: Command) -> Result<Response> {
    Ok(match command {
        Command::Observe(revision) => {
            let snapshot = match revision {
                Some(r) => service.observe_revision(Revision::from_bytes(r)).await?,
                None => service.observe_latest().await?,
            };
            Response::Observation(
                *snapshot.filesystem().as_bytes(),
                *snapshot.root().as_bytes(),
                snapshot.revision().to_bytes(),
            )
        }
        Command::Get(revision, tag, key) => {
            if key.len() > 1024 {
                return Err(protocol());
            }
            let snapshot = service
                .retained_snapshot(Revision::from_bytes(revision))
                .await?;
            Response::Value(snapshot.reader.get(table(tag)?, &key).await?)
        }
        Command::Scan(revision, tag, lower, upper, after, limit) => {
            if limit == 0
                || limit > 4097
                || lower.len() > 1024
                || upper.as_ref().is_some_and(|x| x.len() > 1024)
                || after.as_ref().is_some_and(|x| x.len() > 1024)
            {
                return Err(protocol());
            }
            let snapshot = service
                .retained_snapshot(Revision::from_bytes(revision))
                .await?;
            Response::Rows(
                snapshot
                    .reader
                    .scan(
                        table(tag)?,
                        &lower,
                        upper.as_deref(),
                        after.as_deref(),
                        limit as usize,
                    )
                    .await?,
            )
        }
        Command::Register(bytes) => {
            service
                .register(&ContentDescriptor::from_bytes(&bytes)?)
                .await?;
            Response::Registered
        }
        Command::PrepareOverwrite(revision, node, changes) => {
            if changes.len() > 4096 {
                return Err(protocol());
            }
            let changes = changes
                .iter()
                .map(|(start, end, bytes)| ContentEdit::new(*start..*end, bytes))
                .collect::<Result<Vec<_>>>()?;
            let content = service
                .prepare_overwrite_version(
                    Revision::from_bytes(revision),
                    NodeId::from_bytes(node),
                    &changes,
                )
                .await?;
            Response::Prepared(content.descriptor().to_bytes())
        }
        Command::PrepareUpdate(revision, node, preserved_prefix, final_length, changes) => {
            if changes.len() > 4096 {
                return Err(protocol());
            }
            if preserved_prefix > final_length {
                return Err(Error::invalid(
                    "metadata RPC",
                    "preserved prefix exceeds final length",
                ));
            }
            let changes = changes
                .iter()
                .map(|(start, end, bytes)| ContentEdit::new(*start..*end, bytes))
                .collect::<Result<Vec<_>>>()?;
            if !crate::data::update_is_bounded(preserved_prefix, final_length, &changes) {
                return Err(Error::unsupported(
                    "metadata RPC",
                    "update exceeds bounded work budget",
                ));
            }
            let content = service
                .prepare_update_version(
                    Revision::from_bytes(revision),
                    NodeId::from_bytes(node),
                    preserved_prefix,
                    final_length,
                    &changes,
                )
                .await?;
            Response::Prepared(content.descriptor().to_bytes())
        }
        Command::History(after, limit) => {
            if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
                return Err(protocol());
            }
            let page = service
                .retained_revisions_page(after.map(Revision::from_bytes), limit)
                .await?;
            Response::Revisions(
                page.revisions.into_iter().map(Revision::to_bytes).collect(),
                page.next.map(Revision::to_bytes),
            )
        }
        Command::Expire(bytes) => {
            let request = ExpiryRequest::from_bytes(&bytes)?;
            Response::Expiry(service.expire_revisions(&request).await?.encode()?)
        }
        Command::HistoryStates(after, limit) => {
            if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
                return Err(protocol());
            }
            let page = service
                .history_page(after.map(Revision::from_bytes), limit)
                .await?;
            let entries = page
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(match entry.state {
                        RevisionState::Readable => (entry.revision.to_bytes(), 0, vec![]),
                        RevisionState::Expired(marker) => {
                            (entry.revision.to_bytes(), 1, marker.encode()?)
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Response::RevisionStates(entries, page.next.map(Revision::to_bytes))
        }
        Command::Commit(plans) => {
            if plans.len() > 4096 {
                return Err(protocol());
            }
            Response::Outcomes(
                service
                    .commit_encoded(&plans)
                    .await?
                    .into_iter()
                    .map(|outcome| {
                        Ok(match outcome {
                            Outcome::Committed(receipt) => (0, receipt.encode()?),
                            Outcome::Conflict => (1, vec![]),
                            Outcome::Retryable => (2, vec![]),
                            Outcome::Unknown(id) => (3, id.as_bytes().to_vec()),
                        })
                    })
                    .collect::<Result<_>>()?,
            )
        }
    })
}

/// Authenticated local client with direct OpenDAL content transfer.
#[derive(Clone)]
pub struct ServiceClient {
    addr: SocketAddr,
    token: Arc<str>,
    data: DataStore,
    filesystem: NodeId,
    root: NodeId,
}
impl std::fmt::Debug for ServiceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceClient")
            .field("addr", &self.addr)
            .field("filesystem", &self.filesystem)
            .finish_non_exhaustive()
    }
}
async fn rpc(addr: SocketAddr, token: &str, command: Command) -> Result<Response> {
    tokio::time::timeout(TIMEOUT, async {
        let mut stream = TcpStream::connect(addr).await.map_err(io)?;
        write_frame(
            &mut stream,
            &Request {
                version: crate::compatibility::RPC_PROTOCOL_VERSION,
                token: token.to_owned(),
                command,
            },
        )
        .await?;
        match read_frame(&mut stream).await? {
            Response::Error(tag, message) => {
                Err(Error::new(tag_kind(tag)?, "metadata service", message))
            }
            Response::Expired(revision, message) => Err(Error::expired(
                "metadata service",
                Revision::from_bytes(revision),
                message,
            )),
            response => Ok(response),
        }
    })
    .await
    .map_err(|_| Error::new(ErrorKind::Io, "metadata RPC", "response timed out"))?
}
impl ServiceClient {
    pub async fn connect(addr: SocketAddr, token: String, operator: Operator) -> Result<Self> {
        validate_endpoint(addr, &token)?;
        let Response::Observation(fs, root, _) = rpc(addr, &token, Command::Observe(None)).await?
        else {
            return Err(protocol());
        };
        let marker = operator
            .read(".yinyang/head")
            .await
            .map_err(|e| Error::from_storage("verify service data binding", e))?;
        let (bound_filesystem, bound_root) = super::parse_service_binding(&marker.to_vec())?;
        if bound_filesystem != NodeId::from_bytes(fs) || bound_root != NodeId::from_bytes(root) {
            return Err(Error::invalid(
                "connect service",
                "data binding disagrees with service",
            ));
        }
        Ok(Self {
            addr,
            token: Arc::from(token),
            data: DataStore::new(operator, NodeId::from_bytes(fs))?,
            filesystem: NodeId::from_bytes(fs),
            root: NodeId::from_bytes(root),
        })
    }
    pub fn data(&self) -> &DataStore {
        &self.data
    }
    pub fn filesystem(&self) -> NodeId {
        self.filesystem
    }
    pub fn root(&self) -> NodeId {
        self.root
    }
    async fn observe(&self, revision: Option<Revision>) -> Result<Snapshot> {
        let Response::Observation(fs, root, token) = rpc(
            self.addr,
            &self.token,
            Command::Observe(revision.map(Revision::to_bytes)),
        )
        .await?
        else {
            return Err(protocol());
        };
        if fs != *self.filesystem.as_bytes()
            || root != *self.root.as_bytes()
            || revision.is_some_and(|r| r.to_bytes() != token)
        {
            return Err(protocol());
        }
        let revision = Revision::from_bytes(token);
        Ok(Snapshot {
            data: self.data.clone(),
            filesystem: self.filesystem,
            root: self.root,
            revision,
            reader: Arc::new(RemoteRecords {
                client: self.clone(),
                revision,
            }),
        })
    }
    pub async fn observe_latest(&self) -> Result<Snapshot> {
        self.observe(None).await
    }
    pub async fn observe_revision(&self, revision: Revision) -> Result<Snapshot> {
        self.observe(Some(revision)).await
    }
    pub async fn retained_revisions_page(
        &self,
        after: Option<Revision>,
        limit: u32,
    ) -> Result<RevisionPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(Error::unsupported(
                "list retained revisions",
                format!("page size must be 1..={RETAINED_REVISIONS_PAGE_MAX}"),
            ));
        }
        let Response::Revisions(revisions, next) = rpc(
            self.addr,
            &self.token,
            Command::History(after.map(Revision::to_bytes), limit),
        )
        .await?
        else {
            return Err(protocol());
        };
        if revisions.len() > limit as usize {
            return Err(protocol());
        }
        let revisions = revisions
            .into_iter()
            .map(Revision::from_bytes)
            .collect::<Vec<_>>();
        if revisions.windows(2).any(|window| window[0] >= window[1])
            || after.is_some_and(|cursor| revisions.iter().any(|revision| *revision <= cursor))
        {
            return Err(protocol());
        }
        let next = next.map(Revision::from_bytes);
        if let Some(next) = next
            && (revisions.len() != limit as usize || Some(next) != revisions.last().copied())
        {
            return Err(protocol());
        }
        Ok(RevisionPage { revisions, next })
    }
    pub async fn history_page(&self, after: Option<Revision>, limit: u32) -> Result<HistoryPage> {
        if !(1..=RETAINED_REVISIONS_PAGE_MAX).contains(&limit) {
            return Err(protocol());
        }
        let Response::RevisionStates(entries, next) = rpc(
            self.addr,
            &self.token,
            Command::HistoryStates(after.map(Revision::to_bytes), limit),
        )
        .await?
        else {
            return Err(protocol());
        };
        if entries.len() > limit as usize {
            return Err(protocol());
        }
        let entries = entries
            .into_iter()
            .map(|(revision, state, marker)| {
                let revision = Revision::from_bytes(revision);
                let state = match (state, marker.is_empty()) {
                    (0, true) => RevisionState::Readable,
                    (1, false) => RevisionState::Expired(ExpiryMarker::decode(&marker)?),
                    _ => return Err(protocol()),
                };
                Ok(HistoryEntry { revision, state })
            })
            .collect::<Result<Vec<_>>>()?;
        let page = HistoryPage {
            entries,
            next: next.map(Revision::from_bytes),
        };
        page.validate()?;
        Ok(page)
    }
    pub async fn expire_revisions(&self, request: &ExpiryRequest) -> Result<ExpiryOutcome> {
        request.validate_volume(self.filesystem)?;
        let request_digest = request.digest()?;
        let response = match rpc(self.addr, &self.token, Command::Expire(request.to_bytes()?)).await
        {
            Ok(response) => response,
            Err(error) if matches!(error.kind(), ErrorKind::Io | ErrorKind::Storage) => {
                return Ok(ExpiryOutcome::Unknown(request.id()));
            }
            Err(error) => return Err(error),
        };
        let Response::Expiry(bytes) = response else {
            return Err(protocol());
        };
        let outcome = ExpiryOutcome::decode(&bytes)?;
        if let ExpiryOutcome::Committed(receipt) = &outcome {
            receipt.validate()?;
            if receipt.id != request.id()
                || receipt.request_digest != request_digest
                || receipt.evidence_digest != request.evidence_digest()
                || receipt.before != request.expected_latest()
                || receipt.expired.as_slice() != request.candidates()
            {
                return Err(protocol());
            }
        }
        match &outcome {
            ExpiryOutcome::Unknown(id) if *id != request.id() => return Err(protocol()),
            _ => {}
        }
        Ok(outcome)
    }
    pub async fn prepare(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<PreparedContent> {
        let content = self.data.prepare(source).await?;
        self.register(&content).await?;
        Ok(content)
    }
    pub async fn prepare_overwrite(
        &self,
        base: &FileVersion,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        if base.descriptor().filesystem() != self.filesystem {
            return Err(Error::invalid(
                "prepare service overwrite",
                "base belongs to another filesystem",
            ));
        }
        let changes = edits
            .iter()
            .map(|edit| {
                let range = edit.range();
                (range.start, range.end, edit.bytes().to_vec())
            })
            .collect::<Vec<_>>();
        let response = rpc(
            self.addr,
            &self.token,
            Command::PrepareOverwrite(
                base.revision().to_bytes(),
                *base.node().id().as_bytes(),
                changes,
            ),
        )
        .await?;
        let Response::Prepared(bytes) = response else {
            return Err(protocol());
        };
        let descriptor = ContentDescriptor::from_bytes(&bytes)?;
        if descriptor.filesystem() != self.filesystem
            || descriptor.content_id().length() != base.descriptor().content_id().length()
        {
            return Err(protocol());
        }
        self.data.published(descriptor)
    }
    pub async fn prepare_update(
        &self,
        base: &FileVersion,
        preserved_prefix: u64,
        final_length: u64,
        edits: &[ContentEdit<'_>],
    ) -> Result<PreparedContent> {
        if base.descriptor().filesystem() != self.filesystem {
            return Err(Error::invalid(
                "prepare service update",
                "base belongs to another filesystem",
            ));
        }
        let changes = edits
            .iter()
            .map(|edit| {
                let range = edit.range();
                (range.start, range.end, edit.bytes().to_vec())
            })
            .collect::<Vec<_>>();
        let response = rpc(
            self.addr,
            &self.token,
            Command::PrepareUpdate(
                base.revision().to_bytes(),
                *base.node().id().as_bytes(),
                preserved_prefix,
                final_length,
                changes,
            ),
        )
        .await?;
        let Response::Prepared(bytes) = response else {
            return Err(protocol());
        };
        let descriptor = ContentDescriptor::from_bytes(&bytes)?;
        if descriptor.filesystem() != self.filesystem
            || descriptor.content_id().length() != final_length
        {
            return Err(protocol());
        }
        self.data.published(descriptor)
    }
    pub async fn register(&self, content: &PreparedContent) -> Result<()> {
        let descriptor = self.data.accept(content)?;
        match rpc(
            self.addr,
            &self.token,
            Command::Register(descriptor.to_bytes()),
        )
        .await?
        {
            Response::Registered => Ok(()),
            _ => Err(protocol()),
        }
    }
    pub async fn restore_transaction(&self, bytes: &[u8]) -> Result<Transaction> {
        let (revision, files) = Transaction::inspect(bytes)?;
        let snapshot = self.observe_revision(revision).await?;
        let mut prepared = Vec::new();
        for file in files {
            prepared.push(self.data.import(&file).await?);
        }
        Transaction::restore(bytes, &snapshot, &prepared).await
    }
    pub async fn commit(&self, request: &Transaction) -> Result<Outcome> {
        Ok(self
            .commit_batch(std::slice::from_ref(request))
            .await?
            .remove(0))
    }
    pub async fn commit_batch(&self, requests: &[Transaction]) -> Result<Vec<Outcome>> {
        for request in requests {
            request.validate(self.filesystem)?;
        }
        let plans = requests
            .iter()
            .map(Transaction::to_bytes)
            .collect::<Result<Vec<_>>>()?;
        let response = match rpc(self.addr, &self.token, Command::Commit(plans)).await {
            Ok(response) => response,
            Err(e) if matches!(e.kind(), ErrorKind::Io | ErrorKind::Storage) => {
                return Ok(requests.iter().map(|r| Outcome::Unknown(r.id())).collect());
            }
            Err(e) => return Err(e),
        };
        let Response::Outcomes(outcomes) = response else {
            return Err(protocol());
        };
        if outcomes.len() != requests.len() {
            return Err(protocol());
        }
        outcomes
            .into_iter()
            .zip(requests)
            .map(|((tag, bytes), request)| {
                Ok(match tag {
                    0 => {
                        let receipt = Receipt::decode(&bytes)?;
                        if receipt.commit_id != request.id()
                            || receipt.request_digest != request.digest()
                        {
                            return Err(protocol());
                        }
                        Outcome::Committed(receipt)
                    }
                    1 => Outcome::Conflict,
                    2 => Outcome::Retryable,
                    3 if bytes == request.id().as_bytes() => Outcome::Unknown(request.id()),
                    _ => return Err(protocol()),
                })
            })
            .collect()
    }
}
#[derive(Debug)]
struct RemoteRecords {
    client: ServiceClient,
    revision: Revision,
}
impl RecordReader for RemoteRecords {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            match rpc(
                self.client.addr,
                &self.client.token,
                Command::Get(self.revision.to_bytes(), table as u8, key.to_vec()),
            )
            .await?
            {
                Response::Value(v) => Ok(v),
                _ => Err(protocol()),
            }
        })
    }
    fn scan<'a>(
        &'a self,
        table: Table,
        lower: &'a [u8],
        upper: Option<&'a [u8]>,
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> BoxFuture<'a, Result<Rows>> {
        Box::pin(async move {
            match rpc(
                self.client.addr,
                &self.client.token,
                Command::Scan(
                    self.revision.to_bytes(),
                    table as u8,
                    lower.to_vec(),
                    upper.map(<[u8]>::to_vec),
                    after.map(<[u8]>::to_vec),
                    u32::try_from(limit).map_err(|_| protocol())?,
                ),
            )
            .await?
            {
                Response::Rows(v) => Ok(v),
                _ => Err(protocol()),
            }
        })
    }
}
