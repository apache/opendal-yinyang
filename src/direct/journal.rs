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

//! Durable Direct Mount object generations and mutation intents.

use super::{DirectId, DirectStorageBinding, Error, ErrorKind, Result};
use crate::local::store;
use borsh::{BorshDeserialize, BorshSerialize};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const SCHEMA: &[store::TableColumns<'static>] = &[
    (
        "authority",
        &["singleton", "profile", "binding", "binding_diagnostic"],
    ),
    ("objects", &["id", "key", "record"]),
    ("mutations", &["id", "record"]),
];
pub(super) const PROFILE: store::Profile = store::Profile {
    name: "yinyang-direct-runtime-2",
    table: "authority",
    schema: SCHEMA,
};

pub(super) const OBJECT_STAGED: u8 = 1;
pub(super) const OBJECT_PENDING: u8 = 2;
pub(super) const OBJECT_CONFLICT: u8 = 3;
pub(super) const OBJECT_UNKNOWN: u8 = 4;
pub(super) const OBJECT_FAILED: u8 = 5;
pub(super) const OBJECT_DISCARDED: u8 = 6;

pub(super) const MUTATION_STAGED: u8 = 0;
pub(super) const MUTATION_DISPATCHING: u8 = 1;
pub(super) const MUTATION_APPLIED: u8 = 2;
pub(super) const MUTATION_CONFLICT: u8 = 3;
pub(super) const MUTATION_UNKNOWN: u8 = 4;
pub(super) const MUTATION_DESTINATION_APPLIED: u8 = 5;
pub(super) const MUTATION_PENDING_SOURCE_CLEANUP: u8 = 6;
pub(super) const MUTATION_FAILED: u8 = 7;
pub(super) const MUTATION_SOURCE_DISPATCHING: u8 = 8;

pub(super) const MUTATION_KIND_PUBLISH: u8 = 0;
pub(super) const MUTATION_KIND_CREATE: u8 = 1;
pub(super) const MUTATION_KIND_DELETE: u8 = 2;
pub(super) const MUTATION_KIND_COPY: u8 = 3;
pub(super) const MUTATION_KIND_RENAME: u8 = 4;

const CREATE_SCHEMA: &str = "
    CREATE TABLE authority(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1),
      profile TEXT NOT NULL,
      binding BLOB NOT NULL,
      binding_diagnostic TEXT NOT NULL
    );
    CREATE TABLE objects(
      id BLOB PRIMARY KEY,
      key TEXT NOT NULL UNIQUE,
      record BLOB NOT NULL
    );
    CREATE TABLE mutations(
      id BLOB PRIMARY KEY,
      record BLOB NOT NULL
    );";

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) enum PreconditionRecord {
    Absent,
    Etag(String),
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) struct PinRecord {
    pub version: Option<String>,
    pub etag: Option<String>,
    pub size: u64,
    pub modified: Option<u64>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) enum ObservationRecord {
    Present(PinRecord),
    Missing,
    Unavailable(String),
}

impl PinRecord {
    pub fn modified_time(&self) -> Option<SystemTime> {
        self.modified
            .map(|seconds| SystemTime::UNIX_EPOCH + Duration::from_secs(seconds))
    }
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) struct EndpointRecord {
    pub id: [u8; 16],
    pub key: String,
    pub path: String,
    pub parent: [u8; 16],
    pub name: String,
    pub precondition: PreconditionRecord,
}

impl EndpointRecord {
    pub fn direct_id(&self) -> DirectId {
        DirectId::from_bytes(self.id)
    }
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) struct ObjectRecord {
    pub id: [u8; 16],
    pub key: String,
    pub path: String,
    pub parent: [u8; 16],
    pub name: String,
    pub existed_remote: bool,
    pub precondition: PreconditionRecord,
    pub state: u8,
    pub generation: u64,
    pub durable_generation: u64,
    pub length: u64,
    pub digest: Option<[u8; 32]>,
    pub accepted: Option<PinRecord>,
    pub observed: ObservationRecord,
    pub pending_mutation: Option<[u8; 16]>,
    pub error: Option<String>,
}

impl ObjectRecord {
    pub fn direct_id(&self) -> DirectId {
        DirectId::from_bytes(self.id)
    }
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub(super) struct MutationRecord {
    pub id: [u8; 16],
    pub kind: u8,
    pub state: u8,
    pub generation: u64,
    pub source: Option<EndpointRecord>,
    pub destination: Option<EndpointRecord>,
    pub input_length: u64,
    pub input_digest: Option<[u8; 32]>,
    pub accepted: Option<PinRecord>,
    pub source_observed: ObservationRecord,
    pub destination_observed: ObservationRecord,
    pub error: Option<String>,
}

pub(super) struct Store {
    connection: Connection,
    root: PathBuf,
    _lock: File,
}

pub(super) struct StoreInspection {
    pub profile: String,
    pub binding_id: [u8; 32],
    pub binding_description: String,
    pub objects: [u64; 7],
    pub mutations: [u64; 9],
}

pub(super) fn inspect(
    database: &Path,
    binding: Option<&DirectStorageBinding>,
) -> Result<StoreInspection> {
    let connection = store::open_read_only(database).map_err(store_error)?;
    let (binding_bytes, binding_description) = validate_schema(&connection, binding)?;
    let binding_id: [u8; 32] = binding_bytes.try_into().map_err(|_| {
        Error::new(
            ErrorKind::Storage,
            "Direct runtime binding digest has an invalid length",
        )
    })?;
    let mut objects = [0_u64; 7];
    let mut statement = connection
        .prepare("SELECT record FROM objects")
        .map_err(|error| sqlite("prepare Direct object inspection", error))?;
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|error| sqlite("inspect Direct object records", error))?;
    for row in rows {
        let record: ObjectRecord = decode(
            "Direct object state",
            &row.map_err(|error| sqlite("inspect Direct object records", error))?,
        )?;
        let slot = objects.get_mut(record.state as usize).ok_or_else(|| {
            Error::new(
                ErrorKind::Storage,
                format!("unknown Direct object state {}", record.state),
            )
        })?;
        *slot = slot.saturating_add(1);
    }
    let mut mutations = [0_u64; 9];
    let mut statement = connection
        .prepare("SELECT record FROM mutations")
        .map_err(|error| sqlite("prepare Direct mutation inspection", error))?;
    let rows = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|error| sqlite("inspect Direct mutation records", error))?;
    for row in rows {
        let record: MutationRecord = decode(
            "Direct mutation state",
            &row.map_err(|error| sqlite("inspect Direct mutation records", error))?,
        )?;
        let slot = mutations.get_mut(record.state as usize).ok_or_else(|| {
            Error::new(
                ErrorKind::Storage,
                format!("unknown Direct mutation state {}", record.state),
            )
        })?;
        *slot = slot.saturating_add(1);
    }
    Ok(StoreInspection {
        profile: PROFILE.name.to_owned(),
        binding_id,
        binding_description,
        objects,
        mutations,
    })
}

impl Store {
    pub fn open(root: &Path, binding: &DirectStorageBinding) -> Result<Self> {
        let database = root.join("runtime.db");
        let exists = database
            .try_exists()
            .map_err(|error| local("inspect Direct runtime database", error))?;
        let nonempty = if exists {
            std::fs::metadata(&database)
                .map_err(|error| local("inspect Direct runtime database", error))?
                .len()
                > 0
        } else {
            false
        };
        if nonempty {
            let _ = inspect(&database, Some(binding))?;
        }

        create_private_dir(root)?;
        for child in ["objects", "inputs"] {
            create_private_dir(&root.join(child))?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("runtime.lock"))
            .map_err(|error| local("open Direct runtime lock", error))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::new(
                    ErrorKind::Storage,
                    "Direct runtime state is already owned by another process",
                ));
            }
            Err(TryLockError::Error(error)) => {
                return Err(local("lock Direct runtime state", error));
            }
        }

        let mut connection = store::open_read_write(&database, !nonempty).map_err(store_error)?;

        if nonempty {
            let _ = validate_schema(&connection, Some(binding))?;
        } else {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite("begin Direct runtime initialization", error))?;
            store::initialize(
                &transaction,
                &PROFILE,
                CREATE_SCHEMA,
                &[
                    store::Binding::blob("binding", binding.namespace_id().to_vec()),
                    store::Binding::text("binding_diagnostic", binding.description()),
                ],
            )
            .map_err(store_error)?;
            transaction
                .commit()
                .map_err(|error| sqlite("commit Direct runtime initialization", error))?;
        }
        store::configure(&connection).map_err(store_error)?;
        let store = Self {
            connection,
            root: root.to_owned(),
            _lock: lock,
        };
        store.recover_dispatching()?;
        Ok(store)
    }

    pub fn barrier(&self) -> Result<()> {
        store::barrier(&self.connection).map_err(store_error)
    }

    pub fn object(&self, key: &str) -> Result<Option<ObjectRecord>> {
        self.connection
            .query_row("SELECT record FROM objects WHERE key=?1", [key], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(|error| sqlite("read Direct object state", error))?
            .map(|bytes| decode("Direct object state", &bytes))
            .transpose()
    }

    pub fn object_by_id(&self, id: [u8; 16]) -> Result<Option<ObjectRecord>> {
        self.connection
            .query_row(
                "SELECT record FROM objects WHERE id=?1",
                [id.as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|error| sqlite("read Direct object identity", error))?
            .map(|bytes| decode("Direct object state", &bytes))
            .transpose()
    }

    pub fn objects(&self) -> Result<Vec<ObjectRecord>> {
        let mut statement = self
            .connection
            .prepare("SELECT record FROM objects ORDER BY key")
            .map_err(|error| sqlite("prepare Direct object scan", error))?;
        let rows = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|error| sqlite("scan Direct object state", error))?;
        rows.map(|row| {
            let bytes = row.map_err(|error| sqlite("scan Direct object state", error))?;
            decode("Direct object state", &bytes)
        })
        .collect()
    }

    pub fn mutation(&self, id: [u8; 16]) -> Result<Option<MutationRecord>> {
        self.connection
            .query_row(
                "SELECT record FROM mutations WHERE id=?1",
                [id.as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|error| sqlite("read Direct mutation state", error))?
            .map(|bytes| decode("Direct mutation state", &bytes))
            .transpose()
    }

    pub fn mutations(&self) -> Result<Vec<MutationRecord>> {
        let mut statement = self
            .connection
            .prepare("SELECT record FROM mutations ORDER BY rowid")
            .map_err(|error| sqlite("prepare Direct mutation scan", error))?;
        let rows = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|error| sqlite("scan Direct mutation state", error))?;
        rows.map(|row| {
            let bytes = row.map_err(|error| sqlite("scan Direct mutation state", error))?;
            decode("Direct mutation state", &bytes)
        })
        .collect()
    }

    pub fn put_object(&self, record: &ObjectRecord) -> Result<()> {
        self.put_records(std::slice::from_ref(record), &[])
    }

    pub fn put_mutation(&self, record: &MutationRecord) -> Result<()> {
        self.put_records(&[], std::slice::from_ref(record))
    }

    pub fn put_records(
        &self,
        objects: &[ObjectRecord],
        mutations: &[MutationRecord],
    ) -> Result<()> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(|error| sqlite("begin Direct state update", error))?;
        for object in objects {
            transaction
                .execute(
                    "INSERT OR REPLACE INTO objects(id,key,record) VALUES(?1,?2,?3)",
                    params![
                        object.id.as_slice(),
                        object.key,
                        encode("Direct object state", object)?
                    ],
                )
                .map_err(|error| sqlite("write Direct object state", error))?;
        }
        for mutation in mutations {
            transaction
                .execute(
                    "INSERT OR REPLACE INTO mutations(id,record) VALUES(?1,?2)",
                    params![
                        mutation.id.as_slice(),
                        encode("Direct mutation state", mutation)?
                    ],
                )
                .map_err(|error| sqlite("write Direct mutation state", error))?;
        }
        transaction
            .commit()
            .map_err(|error| sqlite("commit Direct state update", error))
    }

    pub fn remove_object(&self, id: [u8; 16]) -> Result<()> {
        self.connection
            .execute("DELETE FROM objects WHERE id=?1", [id.as_slice()])
            .map_err(|error| sqlite("remove Direct object state", error))?;
        Ok(())
    }

    pub fn data_path(&self, id: [u8; 16]) -> PathBuf {
        self.root.join("objects").join(format!("{}.data", hex(id)))
    }

    pub fn input_path(&self, id: [u8; 16]) -> PathBuf {
        self.root.join("inputs").join(format!("{}.data", hex(id)))
    }

    pub fn temporary_path(&self, directory: &str, id: [u8; 16]) -> PathBuf {
        self.root.join(directory).join(format!("{}.tmp", hex(id)))
    }

    fn recover_dispatching(&self) -> Result<()> {
        let mut mutations = self.mutations()?;
        let mut changed = Vec::new();
        for mutation in &mut mutations {
            if mutation.state == MUTATION_DISPATCHING {
                mutation.state = MUTATION_UNKNOWN;
                mutation.error = Some(
                    "Direct runtime stopped after dispatch began; backend outcome is Unknown"
                        .to_owned(),
                );
                changed.push(mutation.clone());
            } else if mutation.state == MUTATION_SOURCE_DISPATCHING {
                mutation.state = MUTATION_PENDING_SOURCE_CLEANUP;
                mutation.error = Some(
                    "Direct runtime stopped during source cleanup; destination remains accepted"
                        .to_owned(),
                );
                changed.push(mutation.clone());
            }
        }
        let mut objects = self.objects()?;
        let mut changed_objects = Vec::new();
        for object in &mut objects {
            if object.state == OBJECT_PENDING {
                object.state = OBJECT_UNKNOWN;
                object.error = Some(
                    "Direct runtime stopped after publication began; backend outcome is Unknown"
                        .to_owned(),
                );
                changed_objects.push(object.clone());
            }
        }
        if changed.is_empty() && changed_objects.is_empty() {
            return Ok(());
        }
        self.put_records(&changed_objects, &changed)
    }
}

fn validate_schema(
    connection: &Connection,
    binding: Option<&DirectStorageBinding>,
) -> Result<(Vec<u8>, String)> {
    let bindings = binding.map(|binding| {
        [
            store::Binding::blob("binding", binding.namespace_id().to_vec()),
            store::Binding::text("binding_diagnostic", binding.description()),
        ]
    });
    store::validate(
        connection,
        &PROFILE,
        bindings.as_ref().map(|bindings| bindings.as_slice()),
    )
    .map_err(store_error)?;
    let (bound, diagnostic) = connection
        .query_row(
            "SELECT binding,binding_diagnostic FROM authority WHERE singleton=1",
            [],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| sqlite("read Direct runtime profile", error))?;
    Ok((bound, diagnostic))
}

fn create_private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|error| local("create Direct runtime directory", error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| local("protect Direct runtime directory", error))?;
    }
    Ok(())
}

fn encode<T: BorshSerialize>(name: &str, value: &T) -> Result<Vec<u8>> {
    borsh::to_vec(value)
        .map_err(|error| Error::new(ErrorKind::Storage, format!("encode {name}: {error}")))
}

fn decode<T: BorshDeserialize>(name: &str, bytes: &[u8]) -> Result<T> {
    borsh::from_slice(bytes)
        .map_err(|error| Error::new(ErrorKind::Storage, format!("decode {name}: {error}")))
}

fn hex(bytes: [u8; 16]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn local(context: &str, error: std::io::Error) -> Error {
    let kind = ErrorKind::Storage;
    Error::new(kind, format!("{context}: {error}"))
}

fn store_error(error: store::Error) -> Error {
    let kind = match error.kind() {
        store::ErrorKind::Invalid => ErrorKind::Invalid,
        store::ErrorKind::Unsupported => ErrorKind::Unsupported,
        store::ErrorKind::Corrupt => ErrorKind::Corrupt,
        store::ErrorKind::Busy
        | store::ErrorKind::AlreadyExists
        | store::ErrorKind::NotFound
        | store::ErrorKind::ReadOnly
        | store::ErrorKind::PermissionDenied
        | store::ErrorKind::NoSpace
        | store::ErrorKind::Io => ErrorKind::Storage,
    };
    Error::new(kind, error.to_string())
}

fn sqlite(context: &str, error: rusqlite::Error) -> Error {
    let kind = ErrorKind::Storage;
    Error::new(kind, format!("{context}: {error}"))
}
