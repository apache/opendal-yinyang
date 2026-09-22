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

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use yinyang::core::service::ServiceClient;
use yinyang::core::{CommitId, CommitOutcome, Planner};

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an isolated S3 bucket configured with YINYANG_S3_* variables"]
async fn s3_service_process_restart_retains_original_request_and_history() {
    opendal::install_default();
    let temp = tempfile::tempdir().unwrap();
    let root = format!("service-{}", uuid::Uuid::new_v4().simple());
    let token = "test-only-service-token-at-least-32-bytes";
    let config = std::env::vars()
        .filter_map(|(k, v)| {
            k.strip_prefix("YINYANG_S3_")
                .map(|k| (k.to_ascii_lowercase(), v))
        })
        .filter(|(k, _)| k != "root")
        .chain([("root".into(), root.clone())])
        .collect::<Vec<_>>();
    let operator = opendal::Operator::via_iter("s3", config).unwrap();
    let start = || {
        let mut child = Daemon(
            Command::new(env!("CARGO_BIN_EXE_yy"))
                .args(["serve", "--database"])
                .arg(temp.path().join("metadata.db"))
                .args(["--listen", "127.0.0.1:0"])
                .env("YINYANG_S3_ROOT", &root)
                .env("YINYANG_STORAGE_PROFILE", "minio")
                .env("YINYANG_SERVICE_TOKEN", token)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        (child, stdout)
    };
    let connect = |stdout| async {
        let addr = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                let mut line = String::new();
                BufReader::new(stdout).read_line(&mut line).unwrap();
                line.trim()
                    .strip_prefix("metadata service listening on ")
                    .unwrap()
                    .parse()
                    .unwrap()
            }),
        )
        .await
        .unwrap()
        .unwrap();
        ServiceClient::connect(addr, token.into(), operator.clone())
            .await
            .unwrap()
    };
    let (daemon, stdout) = start();
    let client = connect(stdout).await;
    let first = client.observe_latest().await.unwrap();
    let content = client
        .prepare(&mut b"durable service bytes".as_slice())
        .await
        .unwrap();
    let mut plan = Planner::new(&first, CommitId::generate());
    plan.create_file(client.root(), "file", content, false)
        .await
        .unwrap();
    let plan = plan.finish().unwrap();
    let CommitOutcome::Committed(receipt) = client.commit(&plan).await.unwrap() else {
        panic!("not committed")
    };
    let frozen = plan.to_bytes().unwrap();
    drop(daemon); // Kill, do not rely on a graceful checkpoint or destructor.
    let (daemon, stdout) = start();
    let client = connect(stdout).await;
    assert!(
        client
            .observe_revision(first.revision())
            .await
            .unwrap()
            .resolve("file")
            .await
            .unwrap()
            .is_none()
    );
    let restored = client.restore_transaction(&frozen).await.unwrap();
    assert_eq!(
        client.commit(&restored).await.unwrap(),
        CommitOutcome::Committed(receipt)
    );
    let snapshot = client.observe_latest().await.unwrap();
    let node = snapshot.resolve("file").await.unwrap().unwrap();
    let mut bytes = Vec::new();
    client
        .data()
        .read_range(
            snapshot.content(node.id()).await.unwrap().descriptor(),
            0..21,
            &mut bytes,
        )
        .await
        .unwrap();
    assert_eq!(bytes, b"durable service bytes");
    drop(daemon);
}
