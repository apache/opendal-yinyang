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

use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn create_wal_state(path: &Path) -> Connection {
    let writer = Connection::open(path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE authority(profile TEXT);
             CREATE TABLE payload(bytes BLOB);
             INSERT INTO authority VALUES('main-profile');
             INSERT INTO payload VALUES(x'6d61696e');",
        )
        .unwrap();
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    writer
        .execute_batch(
            "UPDATE authority SET profile='wal-profile';
             UPDATE payload SET bytes=x'77616c2d6279746573';",
        )
        .unwrap();
    writer
}

#[test]
fn read_only_wal_reads_committed_frames_without_changing_authoritative_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source.db");
    let writer = create_wal_state(&source);
    let main_before = std::fs::read(&source).unwrap();
    let wal = sidecar(&source, "-wal");
    let wal_before = std::fs::read(&wal).unwrap();
    assert!(!wal_before.is_empty());

    let reader = Connection::open_with_flags(
        &source,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let profile: String = reader
        .query_row("SELECT profile FROM authority", [], |row| row.get(0))
        .unwrap();
    let bytes: Vec<u8> = reader
        .query_row("SELECT bytes FROM payload", [], |row| row.get(0))
        .unwrap();
    assert_eq!(profile, "wal-profile");
    assert_eq!(bytes, b"wal-bytes");
    drop(reader);

    assert_eq!(std::fs::read(&source).unwrap(), main_before);
    assert_eq!(std::fs::read(&wal).unwrap(), wal_before);
    assert!(sidecar(&source, "-shm").exists());
    drop(writer);
}

#[test]
fn immutable_rejects_the_wal_latest_state_invariance_assumption() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source.db");
    let writer = create_wal_state(&source);
    let copy = temp.path().join("copy");
    std::fs::create_dir(&copy).unwrap();
    let database = copy.join("state.db");
    std::fs::copy(&source, &database).unwrap();
    std::fs::copy(sidecar(&source, "-wal"), sidecar(&database, "-wal")).unwrap();

    let immutable = Connection::open_with_flags(
        format!("file:{}?mode=ro&immutable=1", database.display()),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let profile: String = immutable
        .query_row("SELECT profile FROM authority", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        profile, "main-profile",
        "immutable skips committed WAL frames"
    );
    drop(immutable);
    assert!(!sidecar(&database, "-shm").exists());

    let main_before = std::fs::read(&database).unwrap();
    let wal_before = std::fs::read(sidecar(&database, "-wal")).unwrap();
    let reader = Connection::open_with_flags(
        &database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let profile: String = reader
        .query_row("SELECT profile FROM authority", [], |row| row.get(0))
        .unwrap();
    assert_eq!(profile, "wal-profile");
    drop(reader);
    assert_eq!(std::fs::read(&database).unwrap(), main_before);
    assert_eq!(
        std::fs::read(sidecar(&database, "-wal")).unwrap(),
        wal_before
    );
    assert!(sidecar(&database, "-shm").exists());
    drop(writer);
}
