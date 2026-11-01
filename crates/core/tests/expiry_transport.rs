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

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use support::TestBackend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use yinyang_core::service::{MetadataService, ServiceClient, serve};
use yinyang_core::{
    CommitId, CommitOutcome, ErrorKind, ExpiryId, ExpiryOutcome, ExpiryRequest, Planner,
};

fn committed(outcome: CommitOutcome) -> yinyang_core::Receipt {
    match outcome {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_expiry_response_replays_without_advancing_authority() {
    let dir = tempfile::tempdir().unwrap();
    let backend = TestBackend::default();
    let service = MetadataService::create(dir.path().join("metadata.db"), backend.operator())
        .await
        .unwrap();

    let mut first = Planner::new(
        &service.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    first
        .create_directory(service.root(), "kept")
        .await
        .unwrap();
    let first_revision = committed(service.commit(&first.finish().unwrap()).await.unwrap())
        .cursor
        .revision;

    let mut second = Planner::new(
        &service.observe_latest().await.unwrap(),
        CommitId::generate(),
    );
    second
        .create_directory(service.root(), "current")
        .await
        .unwrap();
    committed(service.commit(&second.finish().unwrap()).await.unwrap());
    let before = service.observe_latest().await.unwrap().revision();

    let token = "test-only-service-token-32-bytes-long";
    let server_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = server_listener.local_addr().unwrap();
    let server = tokio::spawn(serve(server_listener, service.clone(), token.into()));

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let drop_next_response = Arc::new(AtomicBool::new(false));
    let proxy = {
        let drop_next_response = drop_next_response.clone();
        tokio::spawn(async move {
            loop {
                let (mut downstream, _) = proxy_listener.accept().await.unwrap();
                let mut upstream = TcpStream::connect(upstream).await.unwrap();

                let length = downstream.read_u32().await.unwrap();
                let mut request = vec![0; length as usize];
                downstream.read_exact(&mut request).await.unwrap();
                upstream.write_u32(length).await.unwrap();
                upstream.write_all(&request).await.unwrap();
                upstream.flush().await.unwrap();

                let length = upstream.read_u32().await.unwrap();
                let mut response = vec![0; length as usize];
                upstream.read_exact(&mut response).await.unwrap();
                if drop_next_response.swap(false, Ordering::SeqCst) {
                    continue;
                }
                downstream.write_u32(length).await.unwrap();
                downstream.write_all(&response).await.unwrap();
                downstream.flush().await.unwrap();
            }
        })
    };

    let client = ServiceClient::connect(proxy_addr, token.into(), backend.operator())
        .await
        .unwrap();
    let request = ExpiryRequest::new(
        service.filesystem(),
        ExpiryId::generate(),
        before,
        vec![first_revision],
        Vec::new(),
        [0x5a; 32],
    )
    .unwrap();

    drop_next_response.store(true, Ordering::SeqCst);
    assert_eq!(
        client.expire_revisions(&request).await.unwrap(),
        ExpiryOutcome::Unknown(request.id())
    );

    let after = service.observe_latest().await.unwrap().revision();
    assert_ne!(after, before);
    assert_eq!(
        service
            .observe_revision(first_revision)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Expired
    );
    let current = service.observe_latest().await.unwrap();
    assert!(current.resolve("kept").await.unwrap().is_some());
    assert!(current.resolve("current").await.unwrap().is_some());

    let receipt = match client.expire_revisions(&request).await.unwrap() {
        ExpiryOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed replay: {other:?}"),
    };
    assert_eq!(receipt.id, request.id());
    assert_eq!(receipt.evidence_digest, request.evidence_digest());
    assert_eq!(receipt.before, before);
    assert_eq!(receipt.after, after);
    assert_eq!(receipt.expired.as_slice(), request.candidates());
    assert_eq!(
        service.observe_latest().await.unwrap().revision(),
        after,
        "exact replay must not publish another maintenance revision"
    );

    proxy.abort();
    server.abort();
}
