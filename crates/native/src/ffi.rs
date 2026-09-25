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

//! The only unsafe boundary: borrowed request bytes in, Rust-owned response out.
#![allow(unsafe_code)]
use crate::{Config, Mode, Request, Session};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::ffi::{CString, c_char};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use yinyang::runtime::{Error, ErrorKind, Result};

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Envelope {
    Connect { config: Config, mode: Mode },
    Call { session: u64, request: Request },
    Close { session: u64 },
}
type Sessions = Mutex<BTreeMap<u64, Arc<Mutex<Session>>>>;
static SESSIONS: OnceLock<Sessions> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);
static EXECUTOR: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

async fn dispatch(call: Envelope) -> Result<Value> {
    let sessions = SESSIONS.get_or_init(Default::default);
    match call {
        Envelope::Connect { config, mode } => {
            let session = Session::connect(config, mode).await?;
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            sessions
                .lock()
                .await
                .insert(id, Arc::new(Mutex::new(session)));
            Ok(json!({"session": id}))
        }
        Envelope::Call { session, request } => {
            let session = sessions
                .lock()
                .await
                .get(&session)
                .cloned()
                .ok_or(Error::State(ErrorKind::Closed, "unknown native session"))?;
            session.lock().await.call(request).await
        }
        Envelope::Close { session } => {
            sessions
                .lock()
                .await
                .remove(&session)
                .ok_or(Error::State(ErrorKind::Closed, "unknown native session"))?;
            Ok(json!({}))
        }
    }
}
fn response(input: &[u8]) -> Value {
    let result = (|| {
        let call = serde_json::from_slice::<Envelope>(input)
            .map_err(|_| Error::Invalid("invalid native request"))?;
        let executor = EXECUTOR.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(4 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("native executor")
        });
        // GCD callback threads can have only a 512 KiB stack. Polling the
        // storage futures there overflows in debug builds. Only wait for the
        // result on the foreign stack; execute the request on Rust workers.
        executor
            .block_on(executor.spawn(dispatch(call)))
            .map_err(|_| Error::State(ErrorKind::Io, "native runtime task failed"))?
    })();
    match result {
        Ok(value) => json!({"ok":value}),
        Err(error) => {
            json!({"error":{"kind":format!("{:?}",error.kind()),"message":error.to_string(),
            "commit":error.commit_id().map(|id|crate::encode(id.as_bytes()))}})
        }
    }
}
/// Execute one bounded request. Calls may run concurrently from foreign threads.
/// Never invoke this blocking entry from a Tokio runtime worker.
///
/// # Safety
/// input must point to length readable bytes for the duration of this call.
/// The returned NUL-terminated UTF-8 allocation must be freed exactly once with
/// yy_native_free, including error responses. It must not be modified or freed
/// by another allocator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn yy_native_call(input: *const u8, length: usize) -> *mut c_char {
    let value = if input.is_null() || length > 8 * 1024 * 1024 {
        json!({"error":{"kind":"Invalid","message":"invalid native input bounds"}})
    } else {
        // SAFETY: readable memory is the caller's explicit C ABI obligation.
        let bytes = unsafe { std::slice::from_raw_parts(input, length) };
        std::panic::catch_unwind(|| response(bytes)).unwrap_or_else(
            |_| json!({"error":{"kind":"Io","message":"native operation panicked"}}),
        )
    };
    // JSON encodes embedded NUL bytes as escapes.
    CString::new(value.to_string())
        .expect("JSON contains no NUL")
        .into_raw()
}
/// Release a response returned by yy_native_call. Null is permitted.
///
/// # Safety
/// response must be null or an unmodified allocation returned by yy_native_call
/// that has not already been freed. No concurrent reader may still reference it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn yy_native_free(response: *mut c_char) {
    if !response.is_null() {
        // SAFETY: ownership is transferred back by the C ABI contract.
        drop(unsafe { CString::from_raw(response) });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_requests_and_released_sessions_are_errors() {
        assert_eq!(response(b"not-json")["error"]["kind"], "Invalid");
        assert_eq!(
            response(br#"{"action":"close","session":999}"#)["error"]["kind"],
            "Closed"
        );
        let request = br#"{"action":"close","session":998}"#;
        // SAFETY: request is valid for its length and this is the only owner.
        unsafe {
            let value = yy_native_call(request.as_ptr(), request.len());
            assert!(!value.is_null());
            let decoded: Value =
                serde_json::from_slice(std::ffi::CStr::from_ptr(value).to_bytes()).unwrap();
            assert_eq!(decoded["error"]["kind"], "Closed");
            yy_native_free(value);
        }
    }
}
