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

//! Synthesized FUSE attributes for both metadata models.

use fuser::{FileAttr, FileType};
use std::time::{Duration, SystemTime};
use yinyang_core::{NodeMode, Revision};

/// One documented metadata freshness window for both FUSE backends.
pub const METADATA_STALENESS: Duration = Duration::from_secs(1);

/// Kernel attribute lifetime. It equals the accepted-snapshot window so a
/// cached FUSE attribute never outlives the metadata observation behind it.
pub const ATTR_TTL: Duration = METADATA_STALENESS;

/// Directory size reported to `stat`; the core stores no directory payload.
pub const DIRECTORY_SIZE: u64 = 4096;

/// Fixed offset makes revision-derived stamps readable without claiming that
/// they are wall-clock times.
const STAMP_EPOCH: u64 = 1_600_000_000;

/// Timestamp source for one immutable observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttributeTime {
    Revision(Revision),
    Storage(SystemTime),
}

/// Backend-observed values before the shared FUSE policy is applied.
#[derive(Clone, Copy, Debug)]
pub struct ObservedAttr {
    pub directory: bool,
    pub mode: NodeMode,
    pub size: u64,
    pub time: AttributeTime,
}

impl ObservedAttr {
    pub fn from_revision(directory: bool, mode: NodeMode, size: u64, revision: Revision) -> Self {
        Self {
            directory,
            mode,
            size,
            time: AttributeTime::Revision(revision),
        }
    }

    pub fn from_storage(directory: bool, size: u64, modified: Option<SystemTime>) -> Self {
        Self {
            directory,
            mode: if directory {
                NodeMode::DIRECTORY
            } else {
                NodeMode::FILE
            },
            size,
            time: AttributeTime::Storage(modified.unwrap_or_else(storage_fallback)),
        }
    }
}

/// Build attributes from one coherent backend observation.
pub fn synthesize(ino: fuser::INodeNo, observed: ObservedAttr, read_only: bool) -> FileAttr {
    let (uid, gid) = mount_owner();
    let stamp = match observed.time {
        AttributeTime::Revision(revision) => {
            SystemTime::UNIX_EPOCH
                + Duration::from_secs(
                    STAMP_EPOCH
                        + u64::from_be_bytes(
                            revision.to_bytes()[..8]
                                .try_into()
                                .expect("revision sequence prefix"),
                        ),
                )
        }
        AttributeTime::Storage(time) => time,
    };
    let size = if observed.directory {
        DIRECTORY_SIZE
    } else {
        observed.size
    };
    let mode = if read_only {
        observed.mode.read_only()
    } else {
        observed.mode
    };
    let perm = mode.bits();
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: stamp,
        mtime: stamp,
        ctime: stamp,
        crtime: stamp,
        kind: if observed.directory {
            FileType::Directory
        } else {
            FileType::RegularFile
        },
        perm,
        nlink: if observed.directory { 2 } else { 1 },
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn storage_fallback() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(STAMP_EPOCH)
}

fn mount_owner() -> (u32, u32) {
    use nix::unistd::{getegid, geteuid};
    (geteuid().as_raw(), getegid().as_raw())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attributes_use_the_observation_time_and_fixed_directory_size() {
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(123);
        let file = synthesize(
            fuser::INodeNo(42),
            ObservedAttr::from_storage(false, 7, Some(modified)),
            true,
        );
        assert_eq!(file.ino, fuser::INodeNo(42));
        assert_eq!(file.size, 7);
        assert_eq!(file.mtime, modified);
        assert_eq!(file.perm, 0o444);

        let directory = synthesize(
            fuser::INodeNo(43),
            ObservedAttr::from_storage(true, 0, None),
            true,
        );
        assert_eq!(directory.size, DIRECTORY_SIZE);
        assert_eq!(
            directory.mtime,
            SystemTime::UNIX_EPOCH + Duration::from_secs(STAMP_EPOCH)
        );
        assert_eq!(directory.perm, 0o555);
    }

    #[test]
    fn attributes_report_persisted_modes_and_mask_writes_when_read_only() {
        let revision = Revision::new(7);
        let writable = synthesize(
            fuser::INodeNo(44),
            ObservedAttr::from_revision(false, NodeMode::from_bits(0o600).unwrap(), 12, revision),
            false,
        );
        assert_eq!(writable.perm, 0o600);

        let read_only = synthesize(
            fuser::INodeNo(45),
            ObservedAttr::from_revision(false, NodeMode::from_bits(0o600).unwrap(), 12, revision),
            true,
        );
        assert_eq!(read_only.perm, 0o400);

        let directory = synthesize(
            fuser::INodeNo(46),
            ObservedAttr::from_revision(true, NodeMode::from_bits(0o700).unwrap(), 0, revision),
            true,
        );
        assert_eq!(directory.perm, 0o500);
    }
}
