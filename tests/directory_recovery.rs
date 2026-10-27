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

//! Real-process acceptance for durable one-shot directory publication.
//!
//! The fixture observes storage independently of `yy`. Its loopback HTTP gate
//! forwards normal S3 traffic and interferes only with the final conditional
//! head publication, so no production-only fault hook is required.

#[path = "linux_sync/harness.rs"]
mod support;

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use support::{Fixture, Publication, RemoteEntry, Tree, local_tree};

const COMMIT_TIMEOUT: Duration = Duration::from_secs(90);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(180);
const MAX_HTTP_HEAD: usize = 64 * 1024;
const MAX_HTTP_BODY: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
enum CommitAction {
    /// Read the complete request, signal interception, and wait before sending
    /// it upstream. This leaves the journal frozen before any commit response.
    HoldBeforeCommit,
    /// Send the request and consume the successful response, then close the
    /// client without forwarding that response. Subsequent connections fail so
    /// the client cannot turn the lost response back into a definitive commit.
    DropResponse,
}

/// A loopback HTTP proxy that intercepts the first `.yinyang/head` mutation.
struct S3Gate {
    address: SocketAddr,
    intercepted: mpsc::Receiver<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl S3Gate {
    fn start(action: CommitAction) -> Self {
        let upstream = upstream_endpoint();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind S3 gate");
        let address = listener.local_addr().expect("S3 gate address");
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let intercepted = Arc::new(AtomicBool::new(false));
        let poisoned = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();
        let thread_release = release.clone();
        let thread_stop = stop.clone();
        let thread_intercepted = intercepted.clone();
        let thread_poisoned = poisoned.clone();
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
                let sender = sender.clone();
                thread::spawn(move || {
                    handle_connection(
                        client,
                        upstream,
                        action,
                        release,
                        intercepted,
                        poisoned,
                        sender,
                    );
                });
            }
        });
        Self {
            address,
            intercepted: receiver,
            release,
            stop,
            thread: Some(thread),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn wait_intercepted(&self) {
        self.intercepted
            .recv_timeout(COMMIT_TIMEOUT)
            .expect("S3 gate did not intercept the conditional head publication");
    }

    fn release(&self) {
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
    mut client: TcpStream,
    upstream: SocketAddr,
    action: CommitAction,
    release: Arc<(Mutex<bool>, Condvar)>,
    intercepted: Arc<AtomicBool>,
    poisoned: Arc<AtomicBool>,
    sender: mpsc::Sender<()>,
) {
    if poisoned.load(Ordering::SeqCst) {
        return;
    }
    let Ok((head, request_line, headers)) = read_http_head(&mut client) else {
        return;
    };
    if !is_head_commit(&request_line) || intercepted.swap(true, Ordering::SeqCst) {
        forward_stream(client, upstream, head).ok();
        return;
    }
    if has_expect_continue(&headers) {
        let _ = client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }
    let Ok(body) = read_http_body(&mut client, &headers) else {
        return;
    };
    let mut request = head;
    request.extend_from_slice(&body);
    if matches!(action, CommitAction::HoldBeforeCommit) {
        let _ = sender.send(());
        wait_release(&release);
    }
    let Ok(mut server) = TcpStream::connect(upstream) else {
        return;
    };
    if server.write_all(&request).is_err() {
        return;
    }
    let Ok(_response) = read_http_response(&mut server) else {
        return;
    };
    if !matches!(action, CommitAction::HoldBeforeCommit) {
        let _ = sender.send(());
    }
    match action {
        CommitAction::HoldBeforeCommit => {}
        CommitAction::DropResponse => {
            poisoned.store(true, Ordering::SeqCst);
        }
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

fn forward_stream(
    mut client: TcpStream,
    upstream: SocketAddr,
    head: Vec<u8>,
) -> std::io::Result<()> {
    let mut server = TcpStream::connect(upstream)?;
    server.write_all(&head)?;
    let mut client_read = client.try_clone()?;
    let mut client_write = client.try_clone()?;
    let mut server_read = server.try_clone()?;
    let mut server_write = server.try_clone()?;
    let request = thread::spawn(move || {
        let _ = std::io::copy(&mut client_read, &mut server_write);
        let _ = server_write.shutdown(Shutdown::Write);
    });
    let response = thread::spawn(move || {
        let _ = std::io::copy(&mut server_read, &mut client_write);
        let _ = client_write.shutdown(Shutdown::Write);
    });
    let _ = request.join();
    let _ = response.join();
    let _ = client.shutdown(Shutdown::Both);
    Ok(())
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

fn read_http_response(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    loop {
        let (head, status_line, headers) = read_http_head(stream)?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "missing status")
            })?;
        let body = read_http_body(stream, &headers)?;
        if !(100..200).contains(&status) {
            let mut response = head;
            response.extend(body);
            return Ok(response);
        }
    }
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
        let size = read_line(stream)?;
        let size = size
            .split(';')
            .next()
            .and_then(|value| usize::from_str_radix(value, 16).ok())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid chunk size")
            })?;
        if size == 0 {
            while !read_line(stream)?.is_empty() {}
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
        body.extend_from_slice(&chunk[..size]);
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

fn is_head_commit(request_line: &str) -> bool {
    let mut parts = request_line.split_whitespace();
    parts.next() == Some("PUT")
        && parts.next().is_some_and(|target| {
            target
                .split('?')
                .next()
                .is_some_and(|path| path.ends_with("/.yinyang/head"))
        })
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

struct Running {
    child: Child,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl Running {
    fn wait(mut self) -> Captured {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            match self.child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                Ok(None) => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("yy did not exit within {PROCESS_TIMEOUT:?}");
                }
                Err(error) => panic!("wait for yy: {error}"),
            }
        };
        Captured {
            status,
            stdout: self.stdout.take().unwrap().join().unwrap_or_default(),
            stderr: self.stderr.take().unwrap().join().unwrap_or_default(),
        }
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(thread) = self.stdout.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.stderr.take() {
            let _ = thread.join();
        }
    }
}

struct Captured {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Captured {
    fn success(self) -> Self {
        assert!(
            self.status.success(),
            "yy failed with {}:\nstdout:\n{}\nstderr:\n{}",
            self.status,
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        );
        self
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.stdout).unwrap_or_else(|error| {
            panic!(
                "parse yy JSON: {error}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&self.stdout),
                String::from_utf8_lossy(&self.stderr)
            )
        })
    }
}

fn spawn(command: &mut Command) -> Running {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn yy");
    let stdout = child.stdout.take().expect("yy stdout is piped");
    let stderr = child.stderr.take().expect("yy stderr is piped");
    Running {
        child,
        stdout: Some(thread::spawn(move || read_all(stdout))),
        stderr: Some(thread::spawn(move || read_all(stderr))),
    }
}

fn offline_yy(args: &[&str]) -> Captured {
    let output = Command::new(env!("CARGO_BIN_EXE_yy"))
        .args(args)
        .env_clear()
        .output()
        .expect("run offline yy");
    Captured {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

fn read_all(mut reader: impl Read) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

fn write_source(root: &Path) {
    std::fs::create_dir_all(root.join("nested/empty-dir")).unwrap();
    std::fs::write(root.join("nested/hello"), b"original").unwrap();
    std::fs::write(root.join("empty"), []).unwrap();
}

fn mutate_source(root: &Path) {
    std::fs::write(root.join("nested/hello"), b"changed after dispatch").unwrap();
    std::fs::remove_file(root.join("empty")).unwrap();
    std::fs::write(root.join("late"), b"new source intent").unwrap();
}

fn publication_inspect(state: &Path, id: &str) -> Value {
    let output = offline_yy(&[
        "publication",
        "inspect",
        id,
        "--state",
        state.to_str().unwrap(),
        "--json",
    ]);
    let output = output.success();
    let value = output.json();
    assert_eq!(value["schema"], "yinyang.publication.inspect.v1");
    assert_eq!(value["intents"].as_array().unwrap().len(), 1);
    value["intents"][0].clone()
}

fn assert_state(intent: &Value, allowed: &[&str]) {
    let state = intent["state"].as_str().expect("inspection state");
    assert!(
        allowed.contains(&state),
        "unexpected durable state {state:?}: {intent:#}"
    );
}

fn assert_tree(actual: &Tree, expected: &Tree, context: &str) {
    assert_eq!(actual, expected, "{context}");
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn response_loss_replays_the_frozen_directory_without_rescanning_source() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    let state = fixture.path("journal");
    let commit_id = uuid::Uuid::new_v4().to_string();
    let gate = S3Gate::start(CommitAction::DropResponse);
    let mut command = fixture.yy_command(&[
        "publish",
        source.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let output = spawn(&mut command).wait();
    assert_eq!(
        output.status.code(),
        Some(3),
        "lost commit response must remain uncertain\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    gate.wait_intercepted();
    gate.release();
    mutate_source(&source);

    let intent = publication_inspect(&state, &commit_id);
    assert_eq!(intent["commit_id"], commit_id);
    assert_state(&intent, &["unknown"]);
    let digest = intent["request_digest"].as_str().unwrap().to_owned();
    assert_eq!(digest.len(), 64);

    let exported = fixture.path("exported");
    fixture.yy_ok(&[
        "publication",
        "export",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
        "--destination",
        exported.to_str().unwrap(),
    ]);
    assert_eq!(local_tree(&exported).unwrap(), original);
    let occupied_export = fixture.yy(&[
        "publication",
        "export",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
        "--destination",
        exported.to_str().unwrap(),
    ]);
    assert!(!occupied_export.status.success());
    assert_eq!(local_tree(&exported).unwrap(), original);

    let retried = fixture.yy_ok(&[
        "publication",
        "retry",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
    ]);
    assert!(
        String::from_utf8_lossy(&retried.stdout).contains(&commit_id),
        "retry output must retain the original commit identity: {}",
        String::from_utf8_lossy(&retried.stdout)
    );
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["committed"]);
    assert_eq!(intent["request_digest"], digest);
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "remote publication",
    );

    let rejected = fixture.yy(&[
        "publish",
        source.to_str().unwrap(),
        "--replace",
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    assert!(!rejected.status.success());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("publication retry"),
        "existing identity must point to source-free retry: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "rejected identity must not rescan changed source",
    );
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn process_death_keeps_the_original_identity_and_missing_receipt_is_uncertain() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    let state = fixture.path("journal");
    let commit_id = uuid::Uuid::new_v4().to_string();
    let gate = S3Gate::start(CommitAction::HoldBeforeCommit);
    let mut command = fixture.yy_command(&[
        "publish",
        source.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit-id",
        &commit_id,
    ]);
    command.env("YINYANG_S3_ENDPOINT", gate.endpoint());
    let running = spawn(&mut command);
    gate.wait_intercepted();

    let receipt = fixture.yy(&["receipt", &commit_id]);
    assert_eq!(receipt.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&receipt.stderr).contains("absence is not proof of failure"),
        "{}",
        String::from_utf8_lossy(&receipt.stderr)
    );
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["prepared", "unknown"]);
    let digest = intent["request_digest"].as_str().unwrap().to_owned();

    mutate_source(&source);
    running.kill();
    gate.release();
    fixture.wait_remote_tree(&original, Duration::from_secs(60));

    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["prepared", "unknown"]);
    assert_eq!(intent["request_digest"], digest);
    fixture.yy_ok(&[
        "publication",
        "retry",
        &commit_id,
        "--state",
        state.to_str().unwrap(),
    ]);
    let intent = publication_inspect(&state, &commit_id);
    assert_state(&intent, &["committed"]);
    assert_eq!(intent["request_digest"], digest);
    assert_tree(
        &fixture.remote_tree().unwrap(),
        &original,
        "recovered publication",
    );

    let revision = fixture.yy_ok(&["receipt", &commit_id]).stdout;
    let revision = std::str::from_utf8(&revision)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("revision: "))
        .unwrap()
        .to_owned();
    let restored = fixture.path("restored");
    fixture.yy_ok(&[
        "restore",
        restored.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert_eq!(local_tree(&restored).unwrap(), original);
}

#[test]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
fn historical_restore_and_publication_export_are_explicit_and_no_clobber() {
    let fixture = Fixture::new(Publication::Object);
    let source = fixture.path("source");
    write_source(&source);
    let original = local_tree(&source).unwrap();
    fixture.yy_ok(&["publish", source.to_str().unwrap()]);
    let first = fixture.yy_ok(&["status"]);
    let revision = std::str::from_utf8(&first.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("revision: "))
        .unwrap()
        .to_owned();

    mutate_source(&source);
    fixture.yy_ok(&["publish", source.to_str().unwrap(), "--replace"]);
    let restored = fixture.path("historical");
    fixture.yy_ok(&[
        "restore",
        restored.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert_eq!(local_tree(&restored).unwrap(), original);

    let existing = fixture.path("occupied");
    std::fs::create_dir(&existing).unwrap();
    std::fs::write(existing.join("marker"), b"keep").unwrap();
    let refused = fixture.yy(&[
        "restore",
        existing.to_str().unwrap(),
        "--revision",
        &revision,
    ]);
    assert!(!refused.status.success());
    assert_eq!(std::fs::read(existing.join("marker")).unwrap(), b"keep");
}
