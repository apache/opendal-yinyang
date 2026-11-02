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

use yinyang::sync_engine::{RemoteCapabilities, RemotePin, SyncModel};

#[test]
fn direct_capabilities_are_the_closed_storage_native_set() {
    let capabilities = RemoteCapabilities::for_model(SyncModel::Direct);
    assert!(!capabilities.directories);
    assert!(!capabilities.executable);
    assert!(!capabilities.batch);
    assert!(!capabilities.rename);
    assert!(!capabilities.delta);
    assert!(!capabilities.receipt_lookup);
    assert_eq!(capabilities.max_batch, 1);
}

#[test]
fn direct_version_pin_is_not_weakened_by_etag_or_mtime() {
    let accepted = RemotePin::Direct {
        observation: [1; 16],
        version: Some("version-1".to_owned()),
        etag: Some("etag-1".to_owned()),
        size: 4,
        modified: Some(1),
    };
    let weaker = RemotePin::Direct {
        observation: [2; 16],
        version: Some("version-1".to_owned()),
        etag: Some("different".to_owned()),
        size: 99,
        modified: Some(99),
    };
    assert!(accepted.matches(&weaker));

    let changed = RemotePin::Direct {
        observation: [2; 16],
        version: Some("version-2".to_owned()),
        etag: Some("different".to_owned()),
        size: 99,
        modified: Some(99),
    };
    assert!(!accepted.matches(&changed));
}
