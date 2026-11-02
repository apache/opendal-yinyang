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

//! Shared Linux open, create and rename flag interpretation.

use fuser::{OpenAccMode, OpenFlags, RenameFlags};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RenameRejection {
    Invalid,
    Unsupported,
}

impl RenameRejection {
    pub(super) fn errno(self) -> fuser::Errno {
        match self {
            Self::Invalid => fuser::Errno::EINVAL,
            Self::Unsupported => fuser::Errno::EOPNOTSUPP,
        }
    }
}

pub(super) fn has_open_flag(flags: i32, flag: nix::fcntl::OFlag) -> bool {
    flags & flag.bits() != 0
}

pub(super) fn writable_open(flags: OpenFlags) -> bool {
    flags.acc_mode() != OpenAccMode::O_RDONLY
}

pub(super) fn writable_create(flags: i32) -> bool {
    flags & libc::O_ACCMODE != libc::O_RDONLY
}

pub(super) fn representable_mode(
    mode: u32,
    umask: u32,
    read_only: bool,
    directory: bool,
    executable: bool,
) -> bool {
    const S_IFMT: u32 = 0o170000;
    const S_IFREG: u32 = 0o100000;
    const S_IFDIR: u32 = 0o040000;
    let file_type = mode & S_IFMT;
    let type_ok = file_type == 0
        || (directory && file_type == S_IFDIR)
        || (!directory && file_type == S_IFREG);
    if !type_ok || mode & !(S_IFMT | 0o777) != 0 {
        return false;
    }
    let effective = mode & !umask & 0o777;
    let expected = if directory || executable {
        if read_only { 0o555 } else { 0o755 }
    } else if read_only {
        0o444
    } else {
        0o644
    };
    effective == expected
}

pub(super) fn rename_replace(flags: RenameFlags) -> Result<bool, RenameRejection> {
    let known =
        RenameFlags::RENAME_NOREPLACE | RenameFlags::RENAME_EXCHANGE | RenameFlags::RENAME_WHITEOUT;
    if flags.bits() & !known.bits() != 0 {
        return Err(RenameRejection::Invalid);
    }
    if flags.contains(RenameFlags::RENAME_EXCHANGE) || flags.contains(RenameFlags::RENAME_WHITEOUT)
    {
        return Err(RenameRejection::Unsupported);
    }
    Ok(!flags.contains(RenameFlags::RENAME_NOREPLACE))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn representable_modes_include_type_bits_but_not_special_bits() {
        assert!(representable_mode(0o100644, 0, false, false, false));
        assert!(representable_mode(0o100755, 0, false, false, true));
        assert!(representable_mode(0o40755, 0, false, true, false));
        assert!(representable_mode(0o100644, 0o022, false, false, false));
        assert!(!representable_mode(0o100600, 0, false, false, false));
        assert!(!representable_mode(
            0o100644 | 0o4000,
            0,
            false,
            false,
            false
        ));
        assert!(!representable_mode(0o020000, 0, false, false, false));
        assert!(!representable_mode(0o40755, 0, false, false, false));
    }

    #[test]
    fn rename_flags_name_supported_replace_semantics() {
        assert!(matches!(rename_replace(RenameFlags::empty()), Ok(true)));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_NOREPLACE),
            Ok(false)
        ));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_EXCHANGE),
            Err(RenameRejection::Unsupported)
        ));
        assert!(matches!(
            rename_replace(RenameFlags::RENAME_WHITEOUT),
            Err(RenameRejection::Unsupported)
        ));
        assert!(matches!(
            rename_replace(RenameFlags::from_bits_retain(1 << 20)),
            Err(RenameRejection::Invalid)
        ));
    }
}
