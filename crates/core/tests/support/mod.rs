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

#![allow(dead_code)]

use opendal::raw::*;
use opendal::{Buffer, BytesRange, EntryMode, Metadata, OperationContext, Operator};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub struct TestBackend {
    pub state: Arc<Mutex<TestState>>,
}

#[derive(Debug, Default)]
pub struct TestState {
    pub objects: BTreeMap<String, StoredObject>,
    pub next_revision: u64,
    pub fail_after_head_write: bool,
    pub stat_calls: u64,
    pub version_write_calls: u64,
    pub fail_data_close: bool,
    pub data_aborts: u64,
    pub file_to_change_on_data_close: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub etag: String,
}

impl TestBackend {
    pub fn operator(&self) -> Operator {
        self.operator_with_streaming_write(true)
    }

    pub fn operator_without_streaming_write(&self) -> Operator {
        self.operator_with_streaming_write(false)
    }

    pub fn operator_with_streaming_write(&self, write_can_multi: bool) -> Operator {
        let service: Servicer = Arc::new(TestService {
            state: self.state.clone(),
            write_can_multi,
        });
        Operator::from_parts(OperationContext::default(), service)
    }

    pub fn fail_next_head_write_after_success(&self) {
        self.state.lock().unwrap().fail_after_head_write = true;
    }

    pub fn stat_calls(&self) -> u64 {
        self.state.lock().unwrap().stat_calls
    }

    pub fn reset_version_write_calls(&self) {
        self.state.lock().unwrap().version_write_calls = 0;
    }

    pub fn version_write_calls(&self) -> u64 {
        self.state.lock().unwrap().version_write_calls
    }

    pub fn version_objects(&self) -> Vec<(String, Vec<u8>)> {
        self.state
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|(path, _)| path.starts_with(".yinyang/versions/"))
            .map(|(path, object)| (path.clone(), object.bytes.clone()))
            .collect()
    }

    pub fn remove_current_version(&self) {
        self.state
            .lock()
            .unwrap()
            .objects
            .retain(|path, _| !path.starts_with(".yinyang/versions/"));
    }

    pub fn corrupt_current_version(&self) {
        let mut state = self.state.lock().unwrap();
        let object = state
            .objects
            .iter_mut()
            .find_map(|(path, object)| path.starts_with(".yinyang/versions/").then_some(object))
            .expect("the test filesystem has a version object");
        object.bytes[0] ^= 1;
    }

    pub fn corrupt_head(&self) {
        let mut state = self.state.lock().unwrap();
        let object = state
            .objects
            .get_mut(".yinyang/head")
            .expect("the test filesystem has a head");
        let checksum = object
            .bytes
            .last_mut()
            .expect("the head contains a checksum");
        *checksum ^= 1;
    }
}

#[derive(Debug)]
struct TestService {
    state: Arc<Mutex<TestState>>,
    write_can_multi: bool,
}

impl Service for TestService {
    type Reader = TestReader;
    type Writer = TestWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("yinyang-test")
    }

    fn capability(&self) -> opendal::Capability {
        opendal::Capability {
            stat: true,
            read: true,
            write: true,
            write_can_multi: self.write_can_multi,
            write_can_empty: true,
            write_with_if_match: true,
            write_with_if_not_exists: true,
            shared: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        Err(unsupported())
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> opendal::Result<RpStat> {
        let mut state = self.state.lock().unwrap();
        state.stat_calls += 1;
        let object = state.objects.get(path).ok_or_else(not_found)?;
        Ok(RpStat::new(metadata(object)))
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> opendal::Result<Self::Reader> {
        let state = self.state.lock().unwrap();
        let object = state.objects.get(path).cloned().ok_or_else(not_found)?;
        Ok(TestReader { object })
    }

    fn write(
        &self,
        _: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        Ok(TestWriter {
            state: self.state.clone(),
            path: path.to_owned(),
            if_match: args.if_match().map(str::to_owned),
            if_not_exists: args.if_not_exists(),
            bytes: Vec::new(),
        })
    }

    fn delete(&self, _: &OperationContext) -> opendal::Result<Self::Deleter> {
        Err(unsupported())
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> opendal::Result<Self::Lister> {
        Err(unsupported())
    }

    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpCopy,
        _: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        Err(unsupported())
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> opendal::Result<RpRename> {
        Err(unsupported())
    }

    async fn presign(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpPresign,
    ) -> opendal::Result<RpPresign> {
        Err(unsupported())
    }
}

#[derive(Debug)]
struct TestReader {
    object: StoredObject,
}

impl oio::Read for TestReader {
    async fn open(
        &self,
        range: BytesRange,
    ) -> opendal::Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (response, buffer) = self.read(range).await?;
        Ok((response, Box::new(buffer)))
    }

    async fn read(&self, range: BytesRange) -> opendal::Result<(RpRead, Buffer)> {
        let range = range.to_content_range(self.object.bytes.len())?;
        Ok((
            RpRead::new(metadata(&self.object)),
            Buffer::from(self.object.bytes[range].to_vec()),
        ))
    }
}

#[derive(Debug)]
struct TestWriter {
    state: Arc<Mutex<TestState>>,
    path: String,
    if_match: Option<String>,
    if_not_exists: bool,
    bytes: Vec<u8>,
}

impl oio::Write for TestWriter {
    async fn write(&mut self, buffer: Buffer) -> opendal::Result<()> {
        if self.path.starts_with(".yinyang/versions/") {
            self.state.lock().unwrap().version_write_calls += 1;
        }
        for chunk in buffer {
            self.bytes.extend_from_slice(&chunk);
        }
        Ok(())
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        let mut state = self.state.lock().unwrap();
        if self.path.starts_with(".yinyang/data/") && state.fail_data_close {
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "data close failed",
            ));
        }
        let current = state.objects.get(&self.path);
        if self.if_not_exists && current.is_some() {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConditionNotMatch,
                "object already exists",
            ));
        }
        if self
            .if_match
            .as_ref()
            .is_some_and(|etag| current.is_none_or(|object| object.etag != *etag))
        {
            return Err(opendal::Error::new(
                opendal::ErrorKind::ConditionNotMatch,
                "ETag does not match",
            ));
        }

        state.next_revision += 1;
        let object = StoredObject {
            bytes: self.bytes.clone(),
            etag: format!("\"{}\"", state.next_revision),
        };
        let metadata = metadata(&object);
        state.objects.insert(self.path.clone(), object);
        if self.path.starts_with(".yinyang/data/")
            && let Some(path) = state.file_to_change_on_data_close.take()
        {
            std::fs::write(path, b"source changed during upload").unwrap();
        }
        if self.path == ".yinyang/head" && state.fail_after_head_write {
            state.fail_after_head_write = false;
            return Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "publication response was lost",
            ));
        }
        Ok(metadata)
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        if self.path.starts_with(".yinyang/data/") {
            self.state.lock().unwrap().data_aborts += 1;
        }
        self.bytes.clear();
        Ok(())
    }
}

fn metadata(object: &StoredObject) -> Metadata {
    Metadata::new(EntryMode::FILE)
        .with_content_length(object.bytes.len() as u64)
        .with_etag(object.etag.clone())
}

fn not_found() -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::NotFound, "object is missing")
}

fn unsupported() -> opendal::Error {
    opendal::Error::new(
        opendal::ErrorKind::Unsupported,
        "operation is not supported by the test backend",
    )
}
