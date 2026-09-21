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
}
#[derive(BorshSerialize, BorshDeserialize)]
enum Response {
    Observation([u8; 16], [u8; 16], [u8; 24]),
    Value(Option<Vec<u8>>),
    Rows(Rows),
    Registered,
    Outcomes(Vec<(u8, Vec<u8>)>),
    Error(u8, String),
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
        _ => return Err(protocol()),
    })
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
                    Ok(request)
                        if request.version == 1
                            && blake3::hash(request.token.as_bytes())
                                == blake3::hash(token.as_bytes()) =>
                    {
                        dispatch(&service, request.command).await
                    }
                    _ => Err(Error::invalid(
                        "metadata RPC",
                        "invalid protocol or authentication",
                    )),
                };
                let response = response.unwrap_or_else(|e| {
                    Response::Error(kind_tag(e.kind()), e.message().to_owned())
                });
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
                .observe_revision(Revision::from_bytes(revision))
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
                .observe_revision(Revision::from_bytes(revision))
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
        Command::Commit(plans) => {
            if plans.len() > 4096 {
                return Err(protocol());
            }
            let mut requests = Vec::new();
            for plan in plans {
                requests.push(service.restore_transaction(&plan).await?);
            }
            Response::Outcomes(
                service
                    .commit_batch(&requests)
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
                version: 1,
                token: token.to_owned(),
                command,
            },
        )
        .await?;
        match read_frame(&mut stream).await? {
            Response::Error(tag, message) => {
                Err(Error::new(tag_kind(tag)?, "metadata service", message))
            }
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
        if marker.to_vec() != encode(&(*b"YYSERV01", fs, root))? {
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
    pub async fn prepare(&self, source: &mut (impl AsyncRead + Unpin)) -> Result<PreparedContent> {
        let content = self.data.prepare(source).await?;
        self.register(&content).await?;
        Ok(content)
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
