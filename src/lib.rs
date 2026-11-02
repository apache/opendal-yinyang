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

//! Apache OpenDAL™ YinYang.
//!
//! A filesystem engine over OpenDAL object storage. Managed volumes keep
//! YinYang Format metadata; Direct volumes use an existing object namespace.
//! Both are reachable through Linux FUSE Mount and ordinary-directory Sync, and
//! Managed volumes also support one-shot directory transfer.
//!
//! See `specs/design-principles.md` for the rules every change follows.

#[cfg(feature = "benchmark-telemetry")]
#[doc(hidden)]
pub mod benchmark;
pub mod direct;
#[cfg(target_os = "linux")]
pub mod direct_sync;
mod directory;
#[cfg(target_os = "linux")]
pub mod fuse;
mod local;
pub mod mount;
#[cfg(target_os = "linux")]
pub mod replica;
pub mod runtime;
pub mod space;
pub mod sync;
pub mod volume;

pub use directory::{
    DirectoryInspection, DirectoryIntent, DirectoryJournal, DirectoryState, DirectoryStatus,
    directory_nodes, prepare_directory, publish_directory, restore_directory,
    restore_directory_revision,
};
pub use space::{
    ManagedRetentionPlan, ManagedSpaceReport, MeasurementState, Unresolved,
    inspect_bound_local_staging, inspect_local_staging, inspect_managed_space,
    plan_managed_retention,
};
pub use yinyang_core as core;

#[cfg(test)]
#[path = "../crates/core/tests/support/mod.rs"]
mod support;
