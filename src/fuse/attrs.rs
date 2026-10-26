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

//! Synthesized FUSE attributes for the YinYang metadata model.

use fuser::{FileAttr, FileType};
use std::time::{Duration, SystemTime};
use yinyang_core::Revision;

/// Short kernel attribute lifetime. This frontend re-observes the authority
/// on demand; a one-second window keeps `stat` churn low without pretending
/// that a longer-lived kernel cache is a coherent remote snapshot.
pub const ATTR_TTL: Duration = Duration::from_secs(1);

/// Directory size reported to `stat`; the core stores no directory payload.
pub const DIRECTORY_SIZE: u64 = 4096;

/// Fixed offset makes revision-derived stamps readable without claiming that
/// they are wall-clock times.
const STAMP_EPOCH: u64 = 1_600_000_000;

/// Build attributes for an immutable observation. `size` must come from the
/// same version as `revision`; handles and snapshots both satisfy that rule.
pub fn attribute(
    ino: fuser::INodeNo,
    directory: bool,
    executable: bool,
    size: u64,
    revision: Revision,
) -> FileAttr {
    let (uid, gid) = mount_owner();
    let stamp = SystemTime::UNIX_EPOCH
        + Duration::from_secs(
            STAMP_EPOCH
                + u64::from_be_bytes(
                    revision.to_bytes()[..8]
                        .try_into()
                        .expect("revision sequence prefix"),
                ),
        );
    let perm = if directory || executable {
        0o555
    } else {
        0o444
    };
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: stamp,
        mtime: stamp,
        ctime: stamp,
        crtime: stamp,
        kind: if directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm,
        nlink: if directory { 2 } else { 1 },
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn mount_owner() -> (u32, u32) {
    use nix::unistd::{getegid, geteuid};
    (geteuid().as_raw(), getegid().as_raw())
}
