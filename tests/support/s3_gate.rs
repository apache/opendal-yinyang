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

//! Loopback S3 fault gate shared by durable recovery acceptance.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const COMMIT_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_HTTP_HEAD: usize = 64 * 1024;
const MAX_HTTP_BODY: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRequest {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
}

impl GateRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Clone, Copy)]
pub enum CommitAction {
    /// Read the complete request, signal interception, and wait before sending
    /// it upstream. This leaves the journal frozen before any commit response.
    HoldBeforeCommit,
    /// Read and hold the request, then close it without sending it upstream.
    DropBeforeCommit,
    /// Send the request and consume the successful response, then close the
    /// client without forwarding that response. Subsequent connections fail so
    /// the client cannot turn the lost response back into a definitive commit.
    DropResponse,
}

enum GateEvent {
    Intercepted,
    CommitSucceeded,
    OriginalDropped,
    CommitFailed(String),
}

#[derive(Clone)]
enum GateTarget {
    HeadCommit,
    Request { method: String, path_suffix: String },
}

impl GateTarget {
    fn matches(&self, request_line: &str) -> bool {
        let mut parts = request_line.split_whitespace();
        let Some(method) = parts.next() else {
            return false;
        };
        let Some(target) = parts.next() else {
            return false;
        };
        match self {
            Self::HeadCommit => {
                method == "PUT"
                    && target
                        .split('?')
                        .next()
                        .is_some_and(|path| path.ends_with("/.yinyang/head"))
            }
            Self::Request {
                method: expected_method,
                path_suffix,
            } => {
                method.eq_ignore_ascii_case(expected_method)
                    && target
                        .split('?')
                        .next()
                        .is_some_and(|path| path.ends_with(path_suffix))
            }
        }
    }
}

/// A loopback HTTP proxy that intercepts the first targeted S3 mutation.
pub struct S3Gate {
    address: SocketAddr,
    events: mpsc::Receiver<GateEvent>,
    release: Arc<(Mutex<bool>, Condvar)>,
    stop: Arc<AtomicBool>,
    intercepted_request: Arc<Mutex<Option<GateRequest>>>,
    thread: Option<JoinHandle<()>>,
}

impl S3Gate {
    pub fn start(action: CommitAction) -> Self {
        Self::start_for(action, GateTarget::HeadCommit)
    }

    /// Intercept the first request whose method and path suffix match.
    ///
    /// `path` is the request path without its query string. Callers whose keys
    /// contain non-ASCII bytes should pass the exact percent-encoded suffix
    /// observed in the signed request.
    pub fn start_for_path(action: CommitAction, method: &str, path: impl Into<String>) -> Self {
        Self::start_for(
            action,
            GateTarget::Request {
                method: method.to_owned(),
                path_suffix: path.into(),
            },
        )
    }

    fn start_for(action: CommitAction, target: GateTarget) -> Self {
        let upstream = upstream_endpoint();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind S3 gate");
        let address = listener.local_addr().expect("S3 gate address");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let intercepted = Arc::new(AtomicBool::new(false));
        let poisoned = Arc::new(AtomicBool::new(false));
        let intercepted_request = Arc::new(Mutex::new(None));
        let (sender, receiver) = mpsc::channel();
        let thread_release = release.clone();
        let thread_stop = stop.clone();
        let thread_intercepted = intercepted.clone();
        let thread_poisoned = poisoned.clone();
        let thread_request = intercepted_request.clone();
        let thread = thread::spawn(move || {
            for incoming in listener.incoming() {
                if thread_stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(client) = incoming else {
                    break;
                };
                let release = thread_release.clone();
                let intercepted = thread_intercepted.clone();
                let poisoned = thread_poisoned.clone();
                let intercepted_request = thread_request.clone();
                let target = target.clone();
                let sender = sender.clone();
                thread::spawn(move || {
                    handle_connection(
                        client,
                        upstream,
                        action,
                        target,
                        release,
                        intercepted,
                        poisoned,
                        intercepted_request,
                        sender,
                    );
                });
            }
        });
        Self {
            address,
            events: receiver,
            release,
            stop,
            intercepted_request,
            thread: Some(thread),
        }
    }

    pub fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn wait_intercepted(&self) -> GateRequest {
        match self
            .events
            .recv_timeout(COMMIT_TIMEOUT)
            .expect("S3 gate did not intercept the targeted publication")
        {
            GateEvent::Intercepted => self
                .intercepted_request
                .lock()
                .expect("S3 gate request lock")
                .clone()
                .expect("S3 gate recorded the intercepted request"),
            GateEvent::CommitSucceeded => {
                panic!("S3 gate observed a commit without reporting request interception")
            }
            GateEvent::OriginalDropped => {
                panic!("S3 gate dropped a request without reporting request interception")
            }
            GateEvent::CommitFailed(error) => panic!("S3 gate commit failed: {error}"),
        }
    }

    pub fn wait_commit_succeeded(&self) {
        match self
            .events
            .recv_timeout(COMMIT_TIMEOUT)
            .expect("S3 gate did not report the forwarded commit result")
        {
            GateEvent::CommitSucceeded => {}
            GateEvent::Intercepted => {
                panic!("S3 gate reported request interception twice")
            }
            GateEvent::OriginalDropped => {
                panic!("S3 gate dropped the request instead of forwarding it")
            }
            GateEvent::CommitFailed(error) => panic!("S3 gate commit failed: {error}"),
        }
    }

    pub fn wait_original_dropped(&self) {
        match self
            .events
            .recv_timeout(COMMIT_TIMEOUT)
            .expect("S3 gate did not report the dropped original request")
        {
            GateEvent::OriginalDropped => {}
            GateEvent::Intercepted => panic!("S3 gate reported request interception twice"),
            GateEvent::CommitSucceeded => {
                panic!("S3 gate forwarded the request that was meant to be dropped")
            }
            GateEvent::CommitFailed(error) => panic!("S3 gate commit failed: {error}"),
        }
    }

    pub fn release(&self) {
        let (released, condvar) = &*self.release;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        condvar.notify_all();
    }
}

impl Drop for S3Gate {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.release();
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_connection(
    client: TcpStream,
    upstream: SocketAddr,
    action: CommitAction,
    target: GateTarget,
    release: Arc<(Mutex<bool>, Condvar)>,
    intercepted: Arc<AtomicBool>,
    poisoned: Arc<AtomicBool>,
    intercepted_request: Arc<Mutex<Option<GateRequest>>>,
    sender: mpsc::Sender<GateEvent>,
) {
    let mut client = client;
    if poisoned.load(Ordering::SeqCst) {
        return;
    }
    let Ok((head, request_line, headers)) = read_http_head(&mut client) else {
        return;
    };
    let is_commit = target.matches(&request_line) && !intercepted.swap(true, Ordering::SeqCst);
    if is_commit {
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let target = parts.next().unwrap_or_default().to_owned();
        *intercepted_request.lock().expect("S3 gate request lock") = Some(GateRequest {
            method,
            target,
            headers: headers.clone(),
        });
    }
    if has_expect_continue(&headers) {
        let _ = client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    let Ok(body) = read_http_body(&mut client, &headers) else {
        return;
    };
    let mut request = head;
    request.extend_from_slice(&body);
    if is_commit
        && matches!(
            action,
            CommitAction::HoldBeforeCommit | CommitAction::DropBeforeCommit
        )
    {
        let _ = sender.send(GateEvent::Intercepted);
        wait_release(&release);
    }
    if is_commit && matches!(action, CommitAction::DropBeforeCommit) {
        let _ = sender.send(GateEvent::OriginalDropped);
        return;
    }
    let Ok(mut server) = TcpStream::connect(upstream) else {
        return;
    };
    if server.write_all(&request).is_err() {
        return;
    }
    let head_only = request_line
        .split_whitespace()
        .next()
        .is_some_and(|method| method == "HEAD");
    let Ok(response) = read_http_response(&mut server, head_only) else {
        return;
    };
    if is_commit {
        if !(200..300).contains(&response.status) {
            let message = format!(
                "targeted S3 mutation returned HTTP {}, expected 2xx",
                response.status
            );
            let _ = sender.send(GateEvent::CommitFailed(message));
            return;
        }
        match action {
            CommitAction::HoldBeforeCommit => {
                let _ = sender.send(GateEvent::CommitSucceeded);
            }
            CommitAction::DropBeforeCommit => unreachable!("handled before forwarding"),
            CommitAction::DropResponse => {
                let _ = sender.send(GateEvent::Intercepted);
                poisoned.store(true, Ordering::SeqCst);
            }
        }
    }
    if !is_commit {
        let response = with_connection_close(&response.bytes);
        let _ = client.write_all(&response);
    }
    let _ = client.shutdown(Shutdown::Both);
}

fn wait_release(release: &(Mutex<bool>, Condvar)) {
    let mut released = release
        .0
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while !*released {
        released = release
            .1
            .wait(released)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn read_http_head(
    stream: &mut TcpStream,
) -> std::io::Result<(Vec<u8>, String, Vec<(String, String)>)> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if bytes.len() == MAX_HTTP_HEAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request head exceeds 64 KiB",
            ));
        }
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 HTTP head"))?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let headers = lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    Ok((bytes, request_line, headers))
}

struct HttpResponse {
    status: u16,
    bytes: Vec<u8>,
}

fn read_http_response(stream: &mut TcpStream, head_only: bool) -> std::io::Result<HttpResponse> {
    loop {
        let (head, status_line, headers) = read_http_head(stream)?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status")
            })?;
        let body = if head_only || matches!(status, 204 | 304) {
            Vec::new()
        } else {
            read_http_body(stream, &headers)?
        };
        if !(100..200).contains(&status) {
            let mut response = head;
            response.extend(body);
            return Ok(HttpResponse {
                status,
                bytes: response,
            });
        }
    }
}

fn with_connection_close(response: &[u8]) -> Vec<u8> {
    let Some(boundary) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return response.to_vec();
    };
    let Ok(head) = std::str::from_utf8(&response[..boundary]) else {
        return response.to_vec();
    };
    let mut output = Vec::with_capacity(response.len() + 19);
    for line in head.split("\r\n") {
        if !line
            .split_once(':')
            .is_some_and(|(name, _)| name.eq_ignore_ascii_case("connection"))
        {
            output.extend_from_slice(line.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
    }
    output.extend_from_slice(b"Connection: close\r\n\r\n");
    output.extend_from_slice(&response[boundary + 4..]);
    output
}

fn read_http_body(
    stream: &mut TcpStream,
    headers: &[(String, String)],
) -> std::io::Result<Vec<u8>> {
    if header(headers, "transfer-encoding").is_some_and(|value| value.contains("chunked")) {
        return read_chunked(stream);
    }
    let length = header(headers, "content-length")
        .map(|value| {
            value.parse::<usize>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid content length")
            })
        })
        .transpose()?
        .unwrap_or(0);
    if length > MAX_HTTP_BODY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "HTTP body exceeds 16 MiB gate bound",
        ));
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn read_chunked(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line = read_line(stream)?;
        let size = line
            .split(';')
            .next()
            .and_then(|value| usize::from_str_radix(value, 16).ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size")
            })?;
        body.extend_from_slice(line.as_bytes());
        body.extend_from_slice(b"\r\n");
        if size == 0 {
            loop {
                let trailer = read_line(stream)?;
                body.extend_from_slice(trailer.as_bytes());
                body.extend_from_slice(b"\r\n");
                if trailer.is_empty() {
                    break;
                }
            }
            return Ok(body);
        }
        if body.len().saturating_add(size) > MAX_HTTP_BODY {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked HTTP body exceeds 16 MiB gate bound",
            ));
        }
        let mut chunk = vec![0; size + 2];
        stream.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
    }
}

fn read_line(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            line.truncate(line.len() - 2);
            return String::from_utf8(line).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 line")
            });
        }
        if line.len() > MAX_HTTP_HEAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP line exceeds 64 KiB",
            ));
        }
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn has_expect_continue(headers: &[(String, String)]) -> bool {
    header(headers, "expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue"))
}

fn upstream_endpoint() -> SocketAddr {
    let endpoint = std::env::var("YINYANG_S3_ENDPOINT")
        .expect("YINYANG_S3_ENDPOINT is required by the isolated S3 acceptance fixture");
    let authority = endpoint
        .strip_prefix("http://")
        .expect("the S3 gate supports the isolated plain-HTTP MinIO fixture")
        .split('/')
        .next()
        .expect("S3 endpoint authority");
    authority
        .parse()
        .expect("S3 endpoint must be an IP socket address")
}
