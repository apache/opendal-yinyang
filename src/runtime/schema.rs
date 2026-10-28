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

//! Exact table and column checks shared by local SQLite profiles.

use rusqlite::Connection;
use std::collections::BTreeSet;

pub(crate) type TableColumns<'a> = (&'a str, &'a [&'a str]);

pub(crate) fn table_names(connection: &Connection) -> rusqlite::Result<BTreeSet<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_master
         WHERE type='table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;
    statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()
}

pub(crate) fn column_names(
    connection: &Connection,
    table: &str,
) -> rusqlite::Result<Option<Vec<String>>> {
    let exists = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1
         )",
        [table],
        |row| row.get::<_, bool>(0),
    )?;
    if !exists {
        return Ok(None);
    }
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(Some(columns))
}

/// Compare the database's ordinary tables and ordered columns with one profile.
///
/// SQLite internal tables are excluded. Column order is part of the signature
/// because maintained writers use positional `INSERT ... VALUES` statements.
pub(crate) fn matches(
    connection: &Connection,
    expected: &[TableColumns<'_>],
) -> rusqlite::Result<bool> {
    let actual_tables = table_names(connection)?;
    let expected_tables = expected
        .iter()
        .map(|(table, _)| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    if actual_tables != expected_tables {
        return Ok(false);
    }
    columns_match(connection, expected)
}

/// Compare only the listed tables and their ordered columns.
pub(crate) fn columns_match(
    connection: &Connection,
    expected: &[TableColumns<'_>],
) -> rusqlite::Result<bool> {
    for (table, expected_columns) in expected {
        let Some(actual_columns) = column_names(connection, table)? else {
            return Ok(false);
        };
        let expected_columns = expected_columns
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>();
        if actual_columns != expected_columns {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_extra_and_reordered_columns() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE sample(first BLOB NOT NULL, second TEXT);
                 CREATE TABLE other(value INTEGER);",
            )
            .unwrap();
        assert!(
            matches(
                &connection,
                &[("sample", &["first", "second"]), ("other", &["value"])]
            )
            .unwrap()
        );
        assert!(
            !matches(
                &connection,
                &[("sample", &["first"]), ("other", &["value"])]
            )
            .unwrap()
        );
        assert!(
            !matches(&connection, &[("sample", &["first", "second"])]).unwrap(),
            "extra tables are not part of the selected profile"
        );
        assert!(
            !matches(
                &connection,
                &[("sample", &["second", "first"]), ("other", &["value"])]
            )
            .unwrap(),
            "positional writers make column order part of the signature"
        );
    }
}
