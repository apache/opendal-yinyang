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

use crate::data::ContentDescriptor;
use crate::{Error, ErrorKind, NodeId, Result};
use unicode_casefold::UnicodeCaseFold as _;
use unicode_normalization::UnicodeNormalization as _;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Link {
    pub parent: NodeId,
    pub name: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeKind {
    Directory { membership: u64 },
    File(ContentDescriptor),
}

/// Persisted ordinary Unix permission bits.
///
/// File-type bits and setuid/setgid/sticky bits are outside the Managed
/// profile. Ownership and ACL state are not represented.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeMode(u16);

impl NodeMode {
    pub const FILE: Self = Self(0o644);
    pub const EXECUTABLE_FILE: Self = Self(0o755);
    pub const DIRECTORY: Self = Self(0o755);

    pub const fn from_bits(bits: u16) -> Option<Self> {
        if bits & !0o777 == 0 {
            Some(Self(bits))
        } else {
            None
        }
    }

    pub const fn from_executable(executable: bool) -> Self {
        Self::FILE.with_executable(executable)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn executable(self) -> bool {
        self.0 & 0o111 != 0
    }

    /// Clear write bits for a read-only presentation. Read and execute bits
    /// retain their stored meaning.
    pub const fn read_only(self) -> Self {
        Self(self.0 & !0o222)
    }

    /// Derive an executable view without discarding existing read/write bits.
    ///
    /// Enabling execute adds the bit for each class that already has read or
    /// write permission. When no class has either, owner execute is the
    /// fallback so the derived view is true. A class with no read/write
    /// permission is never made executable by this compatibility mutation.
    /// Disabling clears all execute bits.
    pub const fn with_executable(self, executable: bool) -> Self {
        let mut bits = self.0 & !0o111;
        if executable {
            let owner = bits & 0o600 != 0;
            let group = bits & 0o060 != 0;
            let other = bits & 0o006 != 0;
            if owner || (!group && !other) {
                bits |= 0o100;
            }
            if group {
                bits |= 0o010;
            }
            if other {
                bits |= 0o001;
            }
        }
        Self(bits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Node {
    pub(crate) id: NodeId,
    pub(crate) generation: u64,
    pub(crate) mode: NodeMode,
    pub(crate) link: Option<Link>,
    pub(crate) kind: NodeKind,
}
type NodeWire = (
    [u8; 16],
    u64,
    u16,
    Option<([u8; 16], String)>,
    u64,
    Option<Vec<u8>>,
);
impl Node {
    pub const fn id(&self) -> NodeId {
        self.id
    }
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    pub const fn mode(&self) -> NodeMode {
        self.mode
    }
    /// Compatibility view for existing consumers: only regular files can be
    /// executable. Directory execute bits remain available through `mode()`.
    pub const fn executable(&self) -> bool {
        matches!(&self.kind, NodeKind::File(_)) && self.mode.executable()
    }
    pub fn link(&self) -> Option<&Link> {
        self.link.as_ref()
    }
    pub fn kind(&self) -> &NodeKind {
        &self.kind
    }
    pub fn is_directory(&self) -> bool {
        matches!(self.kind, NodeKind::Directory { .. })
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let (membership, content) = match &self.kind {
            NodeKind::Directory { membership } => (*membership, None),
            NodeKind::File(file) => (0, Some(file.to_bytes())),
        };
        encode(&(
            *self.id.as_bytes(),
            self.generation,
            self.mode.bits(),
            self.link
                .as_ref()
                .map(|v| (*v.parent.as_bytes(), v.name.clone())),
            membership,
            content,
        ))
    }
    pub(crate) fn decode(bytes: &[u8], fs: NodeId) -> Result<Self> {
        let (id, generation, mode, link, membership, file): NodeWire = decode(bytes)?;
        if generation == 0 {
            return Err(corrupt("zero node generation"));
        }
        let mode = NodeMode::from_bits(mode).ok_or_else(|| corrupt("invalid node mode"))?;
        let kind = match file {
            None if membership > 0 => NodeKind::Directory { membership },
            Some(bytes) if membership == 0 => {
                let content = ContentDescriptor::from_bytes(&bytes)?;
                if content.filesystem() != fs {
                    return Err(corrupt("foreign file reference"));
                }
                NodeKind::File(content)
            }
            _ => return Err(corrupt("invalid node kind")),
        };
        let link = link
            .map(|(parent, name)| {
                name_key(&name).map_err(|_| corrupt("invalid stored name"))?;
                Ok(Link {
                    parent: NodeId::from_bytes(parent),
                    name,
                })
            })
            .transpose()?;
        Ok(Self {
            id: NodeId::from_bytes(id),
            generation,
            mode,
            link,
            kind,
        })
    }
    pub(crate) fn logical(&self) -> Result<Vec<u8>> {
        let content = match &self.kind {
            NodeKind::File(f) => Some((f.content_id().length(), *f.content_id().digest())),
            _ => None,
        };
        let membership = match self.kind {
            NodeKind::Directory { membership } => membership,
            _ => 0,
        };
        encode(&(
            *self.id.as_bytes(),
            self.generation,
            self.mode.bits(),
            self.link
                .as_ref()
                .map(|l| (*l.parent.as_bytes(), l.name.clone())),
            membership,
            content,
        ))
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub name: String,
    pub node_id: NodeId,
}
impl DirectoryEntry {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(&(self.name.clone(), *self.node_id.as_bytes()))
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let (name, id): (String, [u8; 16]) = decode(bytes)?;
        name_key(&name).map_err(|_| corrupt("invalid directory name"))?;
        Ok(Self {
            name,
            node_id: NodeId::from_bytes(id),
        })
    }
}

/// Name normalization is validation, never silent spelling conversion.
pub fn name_key(name: &str) -> Result<String> {
    if name.is_empty()
        || name.len() > 255
        || name == "."
        || name == ".."
        || name.ends_with([' ', '.'])
        || !name.nfc().eq(name.chars())
        || name.chars().any(|c| {
            matches!(c, '\u{0000}'..='\u{001f}' | '\u{007f}'..='\u{009f}')
                || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
        })
    {
        return Err(Error::new(
            ErrorKind::InvalidName,
            "validate name",
            "non-portable or non-NFC component",
        ));
    }
    let folded: String = name.case_fold().nfc().collect();
    let stem = folded.split('.').next().unwrap();
    if matches!(stem, "con" | "prn" | "aux" | "nul")
        || ["com", "lpt"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                matches!(
                    suffix,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
        })
    {
        return Err(Error::new(
            ErrorKind::InvalidName,
            "validate name",
            "reserved device name",
        ));
    }
    Ok(folded)
}
pub(crate) fn entry_key(parent: NodeId, name: &str) -> Result<Vec<u8>> {
    let mut key = parent.as_bytes().to_vec();
    key.extend(name_key(name)?.as_bytes());
    Ok(key)
}
pub(crate) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(v) = upper.pop() {
        if v < 255 {
            upper.push(v + 1);
            return Some(upper);
        }
    }
    None
}
pub(crate) fn encode(value: &impl borsh::BorshSerialize) -> Result<Vec<u8>> {
    borsh::to_vec(value).map_err(|e| Error::invalid("encode metadata", e.to_string()))
}
pub(crate) fn decode<T: borsh::BorshDeserialize>(bytes: &[u8]) -> Result<T> {
    borsh::from_slice(bytes).map_err(|e| corrupt(e.to_string()))
}
pub(crate) fn corrupt(message: impl Into<String>) -> Error {
    Error::corrupt("read namespace", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_modes_validate_and_derive_executable_without_losing_access_bits() {
        assert_eq!(NodeMode::from_bits(0o777).unwrap().bits(), 0o777);
        assert!(NodeMode::from_bits(0o1000).is_none());
        assert!(!NodeMode::from_bits(0o600).unwrap().executable());
        assert_eq!(
            NodeMode::from_bits(0o600)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o700
        );
        assert_eq!(
            NodeMode::from_bits(0o644)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o755
        );
        assert_eq!(
            NodeMode::from_bits(0o604)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o705
        );
        assert_eq!(
            NodeMode::from_bits(0o620)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o730
        );
        assert_eq!(
            NodeMode::from_bits(0o200)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o300
        );
        assert_eq!(
            NodeMode::from_bits(0o020)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o030
        );
        assert_eq!(
            NodeMode::from_bits(0o000)
                .unwrap()
                .with_executable(true)
                .bits(),
            0o100
        );
        assert_eq!(
            NodeMode::from_bits(0o755)
                .unwrap()
                .with_executable(false)
                .bits(),
            0o644
        );
        assert_eq!(
            NodeMode::from_bits(0o600)
                .unwrap()
                .with_executable(false)
                .bits(),
            0o600
        );
    }

    #[test]
    fn read_only_presentation_masks_only_write_bits() {
        assert_eq!(
            NodeMode::from_bits(0o600).unwrap().read_only().bits(),
            0o400
        );
        assert_eq!(
            NodeMode::from_bits(0o644).unwrap().read_only().bits(),
            0o444
        );
        assert_eq!(
            NodeMode::from_bits(0o755).unwrap().read_only().bits(),
            0o555
        );
        assert_eq!(
            NodeMode::from_bits(0o777).unwrap().read_only().bits(),
            0o555
        );
    }

    #[test]
    fn persisted_node_rejects_non_ordinary_mode_bits() {
        let id = NodeId::generate();
        let bytes = encode(&(
            *id.as_bytes(),
            1_u64,
            0o1000_u16,
            Option::<([u8; 16], String)>::None,
            1_u64,
            Option::<Vec<u8>>::None,
        ))
        .unwrap();
        assert_eq!(
            Node::decode(&bytes, NodeId::generate()).unwrap_err().kind(),
            ErrorKind::Corrupt
        );
    }
}
