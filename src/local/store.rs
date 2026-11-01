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

//! Shared SQLite ownership for every local YinYang state profile.

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags, params_from_iter};
use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;
use std::time::Duration;

const BUSY_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) type TableColumns<'a> = (&'a str, &'a [&'a str]);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Profile {
    pub name: &'static str,
    pub table: &'static str,
    pub schema: &'static [TableColumns<'static>],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingValue {
    Blob(Vec<u8>),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Binding {
    pub column: &'static str,
    pub value: BindingValue,
}

impl Binding {
    pub(crate) fn blob(column: &'static str, value: impl Into<Vec<u8>>) -> Self {
        Self {
            column,
            value: BindingValue::Blob(value.into()),
        }
    }

    pub(crate) fn text(column: &'static str, value: impl Into<String>) -> Self {
        Self {
            column,
            value: BindingValue::Text(value.into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorKind {
    Invalid,
    Unsupported,
    Corrupt,
    Busy,
    AlreadyExists,
    NotFound,
    ReadOnly,
    PermissionDenied,
    NoSpace,
    Io,
}

#[derive(Debug)]
pub(crate) struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) const fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) fn open_read_only(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| sqlite("open read-only local database", error))?;
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|error| sqlite("configure read-only local database", error))?;
    connection
        .execute_batch("PRAGMA query_only=ON;")
        .map_err(|error| sqlite("quiesce read-only local database", error))?;
    Ok(connection)
}

pub(crate) fn open_read_write(path: &Path, create: bool) -> Result<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let connection = Connection::open_with_flags(path, flags)
        .map_err(|error| sqlite("open local database", error))?;
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|error| sqlite("configure local database timeout", error))?;
    Ok(connection)
}

pub(crate) fn configure(connection: &Connection) -> Result<()> {
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;",
        )
        .map_err(|error| sqlite("configure local database durability", error))
}

pub(crate) fn is_empty(connection: &Connection) -> Result<bool> {
    table_names(connection).map(|tables| tables.is_empty())
}

pub(crate) fn initialize(
    connection: &Connection,
    profile: &Profile,
    schema: &str,
    bindings: &[Binding],
) -> Result<()> {
    create_schema(connection, schema)?;
    write_binding(connection, profile, bindings)
}

pub(crate) fn create_schema(connection: &Connection, schema: &str) -> Result<()> {
    connection
        .execute_batch(schema)
        .map_err(|error| sqlite("create local database schema", error))
}

pub(crate) fn validate(
    connection: &Connection,
    profile: &Profile,
    bindings: Option<&[Binding]>,
) -> Result<()> {
    let stored = read_profile(connection, profile)?;
    if stored != profile.name {
        return Err(Error::new(
            ErrorKind::Unsupported,
            format!(
                "local database profile {stored:?} is not supported; this build requires {:?}. \
                 Export pending work, then remove or reinitialize the old local state",
                profile.name
            ),
        ));
    }
    if !matches(connection, profile.schema)? {
        return Err(Error::new(
            ErrorKind::Corrupt,
            format!(
                "local database schema does not match profile {:?}; local state formats are not migrated",
                profile.name
            ),
        ));
    }
    if let Some(bindings) = bindings {
        validate_bindings(connection, profile, bindings)?;
    }
    Ok(())
}

pub(crate) fn barrier(connection: &Connection) -> Result<()> {
    // NORMAL commits may remain only in the WAL. A FULL checkpoint syncs the
    // WAL before copying pages and syncs the database after, making every prior
    // commit durable without adding an fsync to each write.
    let (busy, _log, _checkpointed): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(FULL)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| sqlite("checkpoint local database", error))?;
    if busy != 0 {
        return Err(Error::new(
            ErrorKind::Busy,
            "local database checkpoint could not complete",
        ));
    }
    Ok(())
}

pub(crate) fn table_names(connection: &Connection) -> Result<BTreeSet<String>> {
    let mut statement = connection
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type='table' AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .map_err(|error| sqlite("inspect local database tables", error))?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .and_then(|rows| rows.collect())
        .map_err(|error| sqlite("inspect local database tables", error))
}

pub(crate) fn column_names(
    connection: &Connection,
    table: &str,
) -> Result<Option<BTreeSet<String>>> {
    let exists = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1
             )",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|error| sqlite("inspect local database table", error))?;
    if !exists {
        return Ok(None);
    }
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|error| sqlite("inspect local database columns", error))?;
    statement
        .query_map([], |row| row.get::<_, String>(1))
        .and_then(|rows| rows.collect())
        .map(Some)
        .map_err(|error| sqlite("inspect local database columns", error))
}

fn matches(connection: &Connection, expected: &[TableColumns<'_>]) -> Result<bool> {
    let actual_tables = table_names(connection)?;
    let expected_tables = expected
        .iter()
        .map(|(table, _)| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    if actual_tables != expected_tables {
        return Ok(false);
    }
    for (table, expected_columns) in expected {
        let Some(actual_columns) = column_names(connection, table)? else {
            return Ok(false);
        };
        let expected_columns = expected_columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<BTreeSet<_>>();
        if actual_columns != expected_columns {
            return Ok(false);
        }
    }
    Ok(true)
}

fn read_profile(connection: &Connection, profile: &Profile) -> Result<String> {
    let table = profile.table;
    let rows = connection
        .prepare(&format!(
            "SELECT singleton, profile FROM {table} ORDER BY singleton LIMIT 2"
        ))
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .map_err(|error| {
            Error::new(
                ErrorKind::Corrupt,
                format!("read local database profile from {table}: {error}"),
            )
        })?;
    if rows.len() != 1 || rows[0].0 != 1 {
        return Err(Error::new(
            ErrorKind::Corrupt,
            format!(
                "local database table {table} must contain exactly one profile row with singleton=1"
            ),
        ));
    }
    Ok(rows.into_iter().next().expect("checked one row").1)
}

pub(crate) fn write_binding(
    connection: &Connection,
    profile: &Profile,
    bindings: &[Binding],
) -> Result<()> {
    let mut columns = vec!["singleton", "profile"];
    columns.extend(bindings.iter().map(|binding| binding.column));
    let placeholders = (1..=bindings.len() + 1)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "INSERT INTO {}({}) VALUES(1,{placeholders})",
        profile.table,
        columns.join(",")
    );
    let values = std::iter::once(SqlValue::Text(profile.name.to_owned()))
        .chain(bindings.iter().map(|binding| sql_value(&binding.value)))
        .collect::<Vec<_>>();
    connection
        .execute(&sql, params_from_iter(values.iter()))
        .map_err(|error| sqlite("create local database profile row", error))?;
    Ok(())
}

fn validate_bindings(
    connection: &Connection,
    profile: &Profile,
    bindings: &[Binding],
) -> Result<()> {
    for binding in bindings {
        let actual = connection
            .query_row(
                &format!(
                    "SELECT {} FROM {} WHERE singleton=1",
                    binding.column, profile.table
                ),
                [],
                |row| row.get::<_, SqlValue>(0),
            )
            .map_err(|error| {
                Error::new(
                    ErrorKind::Corrupt,
                    format!(
                        "read local database binding {}.{}: {error}",
                        profile.table, binding.column
                    ),
                )
            })?;
        if actual != sql_value(&binding.value) {
            return Err(Error::new(
                ErrorKind::Invalid,
                format!(
                    "local database binding {}.{} does not match this authority or root",
                    profile.table, binding.column
                ),
            ));
        }
    }
    Ok(())
}

fn sql_value(value: &BindingValue) -> SqlValue {
    match value {
        BindingValue::Blob(value) => SqlValue::Blob(value.clone()),
        BindingValue::Text(value) => SqlValue::Text(value.clone()),
    }
}

fn sqlite(context: &str, error: rusqlite::Error) -> Error {
    let kind = match &error {
        rusqlite::Error::QueryReturnedNoRows => ErrorKind::NotFound,
        rusqlite::Error::SqliteFailure(code, _) => match code.code {
            rusqlite::ErrorCode::ConstraintViolation => ErrorKind::AlreadyExists,
            rusqlite::ErrorCode::DiskFull => ErrorKind::NoSpace,
            rusqlite::ErrorCode::ReadOnly => ErrorKind::ReadOnly,
            rusqlite::ErrorCode::PermissionDenied => ErrorKind::PermissionDenied,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                ErrorKind::Busy
            }
            rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase => {
                ErrorKind::Corrupt
            }
            _ => ErrorKind::Io,
        },
        _ => ErrorKind::Io,
    };
    Error::new(kind, format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: Profile = Profile {
        name: "test-profile-2",
        table: "binding",
        schema: &[
            ("binding", &["singleton", "profile", "namespace"]),
            ("rows", &["id", "value"]),
        ],
    };

    #[test]
    fn schema_check_accepts_reordered_columns_but_rejects_a_different_set() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE rows(value BLOB,id BLOB);
                 CREATE TABLE binding(namespace BLOB,profile TEXT,singleton INTEGER);",
            )
            .unwrap();
        assert!(matches(&connection, PROFILE.schema).unwrap());
        connection
            .execute_batch("ALTER TABLE rows ADD COLUMN extra BLOB")
            .unwrap();
        assert!(!matches(&connection, PROFILE.schema).unwrap());
    }

    #[test]
    fn old_profiles_are_unsupported_after_schema_validation() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(
            &connection,
            &PROFILE,
            "CREATE TABLE binding(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                profile TEXT NOT NULL,
                namespace BLOB NOT NULL
             );
             CREATE TABLE rows(id BLOB PRIMARY KEY, value BLOB NOT NULL);",
            &[Binding::blob("namespace", vec![7; 16])],
        )
        .unwrap();
        connection
            .execute("UPDATE binding SET profile='test-profile-1'", [])
            .unwrap();
        let error = validate(
            &connection,
            &PROFILE,
            Some(&[Binding::blob("namespace", vec![7; 16])]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(error.to_string().contains("test-profile-2"));
    }

    #[test]
    fn profile_and_binding_rows_require_exactly_one_current_value() {
        let connection = Connection::open_in_memory().unwrap();
        initialize(
            &connection,
            &PROFILE,
            "CREATE TABLE binding(singleton INTEGER, profile TEXT, namespace BLOB);
             CREATE TABLE rows(id BLOB PRIMARY KEY, value BLOB NOT NULL);",
            &[Binding::blob("namespace", vec![8; 16])],
        )
        .unwrap();
        validate(
            &connection,
            &PROFILE,
            Some(&[Binding::blob("namespace", vec![8; 16])]),
        )
        .unwrap();
        let mismatch = validate(
            &connection,
            &PROFILE,
            Some(&[Binding::blob("namespace", vec![9; 16])]),
        )
        .unwrap_err();
        assert_eq!(mismatch.kind(), ErrorKind::Invalid);

        connection
            .execute(
                "INSERT INTO binding(singleton, profile, namespace) VALUES(2, 'test-profile-2', ?1)",
                [vec![8_u8; 16]],
            )
            .unwrap();
        let malformed = validate(&connection, &PROFILE, None).unwrap_err();
        assert_eq!(malformed.kind(), ErrorKind::Corrupt);
    }
}
