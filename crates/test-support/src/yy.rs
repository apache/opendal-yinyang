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

//! Shared release-binary selector for Linux product-path acceptance.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Select the actual archive binary when YY_UPGRADE_BIN is present.
///
/// Linux acceptance suites share one immutable binary selector so a trial
/// workflow can exercise the same artifact across product paths.
pub fn binary(default: &'static str) -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            let selected = std::env::var_os("YY_UPGRADE_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(default));
            assert!(
                selected.is_file(),
                "YY_UPGRADE_BIN must name a release yy binary: {}",
                selected.display()
            );
            let bytes = std::fs::read(&selected)
                .unwrap_or_else(|error| panic!("read {}: {error}", selected.display()));
            eprintln!(
                "yy acceptance binary: {} blake3={}",
                selected.display(),
                blake3::hash(&bytes).to_hex()
            );
            selected
        })
        .as_path()
}
