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

//! Persistent and wire profile identifiers supported by the core.

pub(crate) const CONTENT_UNIT_BYTES: usize = 64 * 1024;
pub(crate) const CONTENT_DESCRIPTOR_MAGIC: [u8; 8] = *b"YYFILE02";
pub(crate) const CONTENT_INDEX_MAGIC: [u8; 8] = *b"YYHASH02";
pub(crate) const CONTENT_HASH_CONTEXT: &str = "Apache OpenDAL YinYang content profile 2";

pub(crate) const OBJECT_HEAD_MAGIC: [u8; 8] = *b"YYHEAD02";
pub(crate) const OBJECT_SNAPSHOT_MAGIC: [u8; 8] = *b"YYSNAP02";
pub(crate) const OBJECT_SNAPSHOT_EXPIRY_MAGIC: [u8; 8] = *b"YYSNAP03";
pub(crate) const OBJECT_INDEX_MAGIC: [u8; 8] = *b"YYINDEX2";

pub(crate) const REQUEST_MAGIC: [u8; 8] = *b"YYREQ002";
pub(crate) const REQUEST_HASH_CONTEXT: &str = "Apache OpenDAL YinYang request profile 2";
pub(crate) const NODE_IDENTITY_CONTEXT: &str = "Apache OpenDAL YinYang node identity profile 2";
pub(crate) const TRANSACTION_PLAN_MAGIC: [u8; 8] = *b"YYPLAN01";

pub(crate) const SERVICE_BINDING_MAGIC: [u8; 8] = *b"YYSERV01";
pub(crate) const SQLITE_SCHEMA_PROFILE: &str = "yinyang-sqlite-1";
pub(crate) const SQLITE_SCHEMA_PROFILE_EXPIRY: &str = "yinyang-sqlite-2";
pub(crate) const RPC_PROTOCOL_VERSION: u8 = 1;

pub(crate) const EXPIRY_REQUEST_MAGIC: [u8; 8] = *b"YYEXPQ01";
pub(crate) const EXPIRY_RECORD_MAGIC: [u8; 8] = *b"YYEXP01";
pub(crate) const EXPIRY_RECEIPT_MAGIC: [u8; 8] = *b"YYEXPR01";
pub(crate) const EXPIRY_REFUSAL_MAGIC: [u8; 8] = *b"YYEXPF01";
pub(crate) const EXPIRY_HASH_CONTEXT: &str = "Apache OpenDAL YinYang expiry request profile 1";
