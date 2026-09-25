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

#[path = "../../core/tests/support/mod.rs"]
mod support;
use serde_json::{Value, json};
use std::sync::Arc;
use support::TestBackend;
use yinyang::core::{Authority, BackendProfile, Fs};
use yinyang_native::{Mode, Session};

async fn call(session: &mut Session, request: Value) -> Value {
    session
        .call(serde_json::from_value(request).unwrap())
        .await
        .unwrap()
}
#[tokio::test]
async fn mount_and_sync_bridge_share_real_core_contracts() {
    let backend = TestBackend::default();
    let authority: Arc<dyn Authority> = Arc::new(
        Fs::create(backend.operator(), BackendProfile::Minio)
            .await
            .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let mut mount = Session::open(
        authority.clone(),
        &temp.path().join("mount"),
        false,
        Mode::Mount,
    )
    .await
    .unwrap();
    let root = call(&mut mount, json!({"op":"root"})).await["node"].clone();
    let item = call(
        &mut mount,
        json!({"op":"create","parent":root,"name":"a","directory":false}),
    )
    .await;
    let node = item["node"].clone();
    call(
        &mut mount,
        json!({"op":"write","node":node,"offset":0,"bytes":[65,66,67]}),
    )
    .await;
    assert_eq!(
        call(
            &mut mount,
            json!({"op":"read","node":node,"offset":0,"length":3})
        )
        .await["bytes"],
        json!([65, 66, 67])
    );
    call(&mut mount, json!({"op":"fsync"})).await;
    call(
        &mut mount,
        json!({"op":"create","parent":root,"name":"c","directory":false}),
    )
    .await;
    let page = call(
        &mut mount,
        json!({"op":"scan","node":root,"offset":0,"limit":1}),
    )
    .await;
    call(
        &mut mount,
        json!({"op":"create","parent":root,"name":"b","directory":false}),
    )
    .await;
    let next = call(
        &mut mount,
        json!({"op":"scan","node":root,"revision":page["revision"],"offset":1,"limit":1}),
    )
    .await;
    assert_eq!(next["entries"][0]["name"], "c");
    let changes = call(
        &mut mount,
        json!({"op":"changes","revision":page["revision"],"ordinal":u32::MAX,"limit":1}),
    )
    .await;
    assert!(
        changes["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["after"]["name"] == "b")
    );
    let end = call(
        &mut mount,
        json!({"op":"changes","revision":changes["revision"],"ordinal":changes["ordinal"],"limit":1}),
    ).await;
    assert_eq!(end["changes"], json!([]));
    assert_eq!(end["more"], false);
    call(&mut mount, json!({"op":"release","node":node})).await;
    let latest = call(&mut mount, json!({"op":"node","node":node})).await;
    let mut sync = Session::open(authority, &temp.path().join("sync"), false, Mode::Sync)
        .await
        .unwrap();
    let pinned = call(
        &mut sync,
        json!({"op":"download","node":node,"revision":latest["revision"],"offset":1,"length":1}),
    )
    .await;
    assert_eq!(pinned["bytes"], json!([66]));
    let path = temp.path().join("edit");
    tokio::fs::write(&path, b"edited").await.unwrap();
    let edit = call(
        &mut sync,
        json!({"op":"stage","node":node,"revision":latest["revision"],"path":path}),
    )
    .await;
    call(&mut sync, json!({"op":"publish","edit":edit["edit"]})).await;
    assert_eq!(
        call(&mut sync, json!({"op":"edit_status","edit":edit["edit"]})).await["pending"],
        false
    );
    assert_eq!(
        call(
            &mut sync,
            json!({"op":"download","node":node,"revision":latest["revision"],"offset":0,"length":3})
        )
        .await["bytes"],
        json!([65, 66, 67])
    );
}
