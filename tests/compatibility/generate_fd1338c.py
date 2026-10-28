#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Produce immutable cross-version fixtures with the pinned fd1338c yy binary."""

import argparse
import hashlib
import hmac
import http.client
import json
import os
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

EXPECTED_PRODUCER = "fd1338c2f924a90e9da752b9158e748d1d3acaf4"
SCHEMA = "yinyang.compatibility.fixtures.v1"
SERVICE_TOKEN = "compatibility-service-token-0123456789abcdef"
OBJECT_COMMIT_V1 = "00000000-0000-4000-8000-000000000101"
OBJECT_COMMIT_V2 = "00000000-0000-4000-8000-000000000102"
OBJECT_COMMIT_JOURNAL = "00000000-0000-4000-8000-000000000103"
SYNC_INITIAL_COMMIT = "00000000-0000-4000-8000-000000000201"


def fail(message):
    raise SystemExit(message)


def run(command, env, cwd=None, check=True):
    print("+", " ".join(str(value) for value in command), flush=True)
    completed = subprocess.run(
        [str(value) for value in command],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and completed.returncode != 0:
        fail(
            f"command failed with {completed.returncode}: {command}\n"
            f"stdout:\n{completed.stdout.decode(errors='replace')}\n"
            f"stderr:\n{completed.stderr.decode(errors='replace')}"
        )
    return completed


def text(bytes_value):
    return bytes_value.decode("utf-8")


def revision(output):
    for line in text(output.stdout).splitlines():
        if line.startswith("revision: "):
            return line.removeprefix("revision: ")
    fail(f"command did not report a revision:\n{text(output.stdout)}")


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sqlite_backup(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
        result = destination_connection.execute("PRAGMA integrity_check").fetchone()
        if result != ("ok",):
            fail(f"SQLite backup integrity check failed for {destination}: {result}")
    finally:
        destination_connection.close()
        source_connection.close()


def copy_sqlite_state(source, destination, databases):
    databases = {Path(relative) for relative in databases}
    destination.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if (
            relative in databases
            or path.name.endswith(("-wal", "-shm"))
            or path.name.endswith(".lock")
        ):
            continue
        target = destination / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        else:
            fail(f"SQLite state contains unsupported path {path}")
    for relative in databases:
        sqlite_backup(source / relative, destination / relative)


def head_identity(path):
    data = path.read_bytes()
    if data[:8] == b"YYHEAD02":
        if len(data) < 65:
            fail(f"{path} is a truncated YYHEAD02 fixture")
        return {
            "profile": "YYHEAD02",
            "mode": data[8],
            "filesystem": data[9:25].hex(),
            "root": data[25:41].hex(),
            "revision": data[41:65].hex(),
        }
    if data[:8] == b"YYSERV01":
        if len(data) != 40:
            fail(f"{path} is a malformed YYSERV01 fixture")
        return {
            "profile": "YYSERV01",
            "filesystem": data[8:24].hex(),
            "root": data[24:40].hex(),
        }
    fail(f"{path} does not contain a supported fd1338c authority marker")


class S3:
    def __init__(self):
        endpoint = os.environ["YINYANG_S3_ENDPOINT"]
        if not endpoint.startswith("http://"):
            fail("fixture generation supports the isolated plain-HTTP Silo endpoint only")
        self.host = endpoint.removeprefix("http://").rstrip("/")
        self.bucket = os.environ["YINYANG_S3_BUCKET"]
        self.region = os.environ["YINYANG_S3_REGION"]
        self.access_key = os.environ["YINYANG_S3_ACCESS_KEY_ID"]
        self.secret_key = os.environ["YINYANG_S3_SECRET_ACCESS_KEY"]

    @staticmethod
    def quote(value, keep_slash=False):
        safe = "-_.~" + ("/" if keep_slash else "")
        return urllib.parse.quote(value, safe=safe)

    def request(self, method, key=None, query=None, body=b""):
        canonical_query = "&".join(
            f"{self.quote(name)}={self.quote(value)}"
            for name, value in sorted((query or {}).items())
        )
        canonical_uri = f"/{self.quote(self.bucket)}"
        if key is not None:
            canonical_uri += f"/{self.quote(key, keep_slash=True)}"
        now = time.gmtime()
        date = time.strftime("%Y%m%d", now)
        amz_date = time.strftime("%Y%m%dT%H%M%SZ", now)
        payload_hash = hashlib.sha256(body).hexdigest()
        canonical_headers = (
            f"host:{self.host}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = (
            f"{method}\n{canonical_uri}\n{canonical_query}\n"
            f"{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )
        scope = f"{date}/{self.region}/s3/aws4_request"
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n"
            f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        )

        def sign(key, value):
            return hmac.new(key, value, hashlib.sha256).digest()

        date_key = sign(f"AWS4{self.secret_key}".encode(), date.encode())
        region_key = sign(date_key, self.region.encode())
        service_key = sign(region_key, b"s3")
        signing_key = sign(service_key, b"aws4_request")
        signature = hmac.new(
            signing_key, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        host, port = (self.host.rsplit(":", 1) + [80])[:2]
        connection = http.client.HTTPConnection(host, int(port), timeout=60)
        target = canonical_uri
        if canonical_query:
            target += f"?{canonical_query}"
        connection.request(
            method,
            target,
            body=body,
            headers={
                "Host": self.host,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": amz_date,
                "Authorization": authorization,
            },
        )
        response = connection.getresponse()
        value = response.read()
        headers = {name.lower(): item for name, item in response.getheaders()}
        connection.close()
        return response.status, headers, value

    def copy_prefix(self, prefix, destination):
        token = None
        keys = []
        while True:
            query = {
                "encoding-type": "url",
                "list-type": "2",
                "prefix": prefix,
            }
            if token is not None:
                query["continuation-token"] = token
            status, _, body = self.request("GET", query=query)
            if status != 200:
                fail(f"S3 LIST {prefix!r} failed with {status}: {text(body)}")
            root = ET.fromstring(body)
            namespace = ""
            if root.tag.startswith("{"):
                namespace = root.tag.split("}", 1)[0] + "}"
            for item in root.findall(f"{namespace}Contents"):
                key = urllib.parse.unquote(item.findtext(f"{namespace}Key"))
                keys.append(key)
            token = root.findtext(f"{namespace}NextContinuationToken")
            if not token:
                break
        for key in sorted(keys):
            relative = key.removeprefix(prefix).lstrip("/")
            if not relative or relative == key:
                fail(f"S3 key {key!r} is outside fixture prefix {prefix!r}")
            status, _, body = self.request("GET", key=key)
            if status != 200:
                fail(f"S3 GET {key!r} failed with {status}: {text(body)}")
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
        return keys


def storage_env(root, extra=None):
    env = os.environ.copy()
    env.update(
        {
            "YINYANG_S3_ROOT": root,
            "YINYANG_STORAGE_PROFILE": "minio",
            "YINYANG_SERVICE_TOKEN": SERVICE_TOKEN,
        }
    )
    if extra:
        env.update(extra)
    return env


def unique_root(kind):
    return f"compat-fixture-{kind}-{time.time_ns()}"


def write_config(path, name, access, frontend, publication, staging=None, service=None):
    document = {
        "name": name,
        "model": "managed",
        "access": access,
        "frontend": frontend,
        "publication": publication,
        "storage_profile": "minio",
        "read_only": False,
        "require": [],
    }
    if staging is not None:
        document["staging"] = str(staging)
    if service is not None:
        document["service_address"] = service
    write(path, json.dumps(document, indent=2).encode())


def parse_handle(output):
    value = text(output.stdout).strip().splitlines()[0]
    if len(value) != 36:
        fail(f"unexpected file handle output: {value!r}")
    return value


def local_tree(root):
    entries = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            entries.append({"path": relative, "kind": "directory"})
        elif stat.S_ISREG(metadata.st_mode):
            entries.append(
                {
                    "path": relative,
                    "kind": "file",
                    "length": metadata.st_size,
                    "sha256": sha256_file(path),
                    "executable": bool(metadata.st_mode & stat.S_IXUSR),
                }
            )
        else:
            fail(f"fixture source contains unsupported path {path}")
    return entries


def source_tree(root):
    (root / "nested" / "empty-dir").mkdir(parents=True)
    write(root / "hello.txt", b"hello from fd1338c\n")
    write(root / "nested" / "empty", b"")
    write(root / "nested" / "name-\u03bb.txt", b"unicode payload")
    write(root / "literal%.txt", b"literal percent")
    script = root / "run.sh"
    write(script, b"#!/bin/sh\nprintf 'fd1338c\\n'\n")
    script.chmod(0o755)


def mutate_source(root):
    write(root / "hello.txt", b"hello from fd1338c v2\n")
    write(root / "second.txt", b"second published file")
    write(root / "nested" / "name-\u03bb.txt", b"unicode payload v2")


def create_object_fixture(binary, output, s3, temp):
    root = unique_root("object")
    env = storage_env(root)
    work = temp / "object-work"
    work.mkdir()
    h1 = run([binary, "create"], env)
    source1 = work / "source-v1"
    source_tree(source1)
    p1 = run(
        [binary, "publish", source1, "--commit-id", OBJECT_COMMIT_V1],
        env,
    )
    revision1 = revision(run([binary, "status"], env))
    source2 = work / "source-v2"
    shutil.copytree(source1, source2)
    mutate_source(source2)
    p2 = run(
        [binary, "publish", source2, "--replace", "--commit-id", OBJECT_COMMIT_V2],
        env,
    )
    revision2 = revision(run([binary, "status"], env))
    restored = work / "restored-v1"
    run([binary, "restore", restored, "--revision", revision1], env)
    if local_tree(restored) != local_tree(source1):
        fail("fd1338c did not restore its own historical object revision")

    journal_source = work / "journal-source"
    source_tree(journal_source)
    write(journal_source / "journal-only.txt", b"directory journal fixture")
    journal = work / "journal"
    journal_publish = run(
        [
            binary,
            "publish",
            journal_source,
            "--replace",
            "--state",
            journal,
            "--commit-id",
            OBJECT_COMMIT_JOURNAL,
        ],
        env,
    )
    journal_revision = revision(
        run([binary, "receipt", OBJECT_COMMIT_JOURNAL], env)
    )

    object_config = work / "object-volume.json"
    staging = work / "stage-pending"
    write_config(
        object_config,
        "compat-object-stage",
        "mount",
        "library",
        "object",
        staging,
    )
    run(
        [binary, "--volume", object_config, "file", "create", "pending.txt"],
        env,
    )
    handle = parse_handle(
        run(
            [
                binary,
                "--volume",
                object_config,
                "file",
                "open",
                "pending.txt",
                "--write",
            ],
            env,
        )
    )
    pending_bytes = work / "pending-input"
    write(pending_bytes, b"pending bytes produced by fd1338c")
    run(
        [
            binary,
            "--volume",
            object_config,
            "file",
            "write",
            handle,
            pending_bytes,
        ],
        env,
    )
    status = text(
        run(
            [binary, "--volume", object_config, "status"],
            env,
        ).stdout
    )
    if "pending: true" not in status or handle not in status:
        fail(f"fd1338c pending staging fixture was not created:\n{status}")

    destination = output / "object"
    keys = s3.copy_prefix(f"{root}/", destination / "storage")
    shutil.copytree(source1, destination / "source-v1")
    shutil.copytree(source2, destination / "source-v2")
    shutil.copytree(journal_source, destination / "journal-source")
    copy_sqlite_state(journal, destination / "journal", ["directory.db"])
    copy_sqlite_state(staging, destination / "stage-pending", ["staging.db"])
    write_config(
        destination / "object-volume.json",
        "compat-object-stage",
        "mount",
        "library",
        "object",
        destination / "stage-pending",
    )
    expected = {
        "storage_keys": [key.removeprefix(f"{root}/") for key in keys],
        "source_v1": local_tree(source1),
        "source_v2": local_tree(source2),
        "journal_source": local_tree(journal_source),
        "object": {
            "head": head_identity(destination / "storage" / ".yinyang/head"),
            "revision_v1": revision1,
            "revision_v2": revision2,
            "commit_v1": OBJECT_COMMIT_V1,
            "commit_v2": OBJECT_COMMIT_V2,
            "publish_v1_stdout": text(p1.stdout),
            "publish_v2_stdout": text(p2.stdout),
        },
        "journal": {
            "commit": OBJECT_COMMIT_JOURNAL,
            "revision": journal_revision,
            "publish_stdout": text(journal_publish.stdout),
            "source": local_tree(journal_source),
        },
        "stage_pending": {
            "handle": handle,
            "bytes_sha256": hashlib.sha256(
                b"pending bytes produced by fd1338c"
            ).hexdigest(),
            "status": status,
        },
    }
    write(destination / "expected.json", json.dumps(expected, indent=2).encode())


def start_service(binary, database, root, env):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    address = listener.getsockname()
    listener.close()
    listen = f"{address[0]}:{address[1]}"
    command = [
        str(binary),
        "serve",
        "--database",
        str(database),
        "--listen",
        listen,
    ]
    process = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    line = process.stdout.readline()
    if not line.startswith(b"metadata service listening on "):
        process.kill()
        stdout, stderr = process.communicate()
        fail(
            f"fd1338c service did not start:\n{text(line + stdout)}\n"
            f"{text(stderr)}"
        )
    return process, listen


def stop_service(process):
    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=30)
    if process.returncode not in (0, -signal.SIGTERM):
        fail(
            f"fd1338c service exited with {process.returncode}:\n"
            f"{text(stdout)}\n{text(stderr)}"
        )


def create_service_fixture(binary, output, s3, temp):
    root = unique_root("service")
    env = storage_env(root)
    work = temp / "service-work"
    work.mkdir()
    database = work / "metadata.db"
    process, listen = start_service(binary, database, root, env)
    try:
        config = work / "service-volume.json"
        staging = work / "service-staging"
        write_config(
            config,
            "compat-service",
            "mount",
            "library",
            "service",
            staging,
            listen,
        )
        run(
            [binary, "--volume", config, "file", "create", "service.txt"],
            env,
        )
        handle = parse_handle(
            run(
                [
                    binary,
                    "--volume",
                    config,
                    "file",
                    "open",
                    "service.txt",
                    "--write",
                ],
                env,
            )
        )
        payload = work / "service-input"
        write(payload, b"service bytes produced by fd1338c")
        run(
            [
                binary,
                "--volume",
                config,
                "file",
                "write",
                handle,
                payload,
            ],
            env,
        )
        published = run(
            [binary, "--volume", config, "file", "fsync", handle],
            env,
        )
        run(
            [binary, "--volume", config, "file", "close", handle],
            env,
        )
        service_status = text(
            run([binary, "--volume", config, "status"], env).stdout
        )
    finally:
        stop_service(process)

    destination = output / "service"
    keys = s3.copy_prefix(f"{root}/", destination / "storage")
    sqlite_backup(database, destination / "metadata.db")
    write_config(
        destination / "service-volume.json",
        "compat-service",
        "mount",
        "library",
        "service",
        destination / "service-staging",
        "<restart-address>",
    )
    expected = {
        "storage_keys": [key.removeprefix(f"{root}/") for key in keys],
        "handle": handle,
        "bytes_sha256": hashlib.sha256(
            b"service bytes produced by fd1338c"
        ).hexdigest(),
        "fsync_stdout": text(published.stdout),
        "status": service_status,
        "head": head_identity(destination / "storage" / ".yinyang/head"),
    }
    write(destination / "expected.json", json.dumps(expected, indent=2).encode())


def create_sync_fixture(binary, output, s3, temp):
    root = unique_root("sync")
    env = storage_env(root)
    work = temp / "sync-work"
    work.mkdir()
    run([binary, "create"], env)
    config = work / "sync-volume.json"
    state = work / "sync-state"
    local = work / "local"
    local.mkdir()
    write_config(
        config,
        "compat-sync",
        "sync",
        "library",
        "object",
        state,
    )
    run(
        [binary, "--volume", config, "sync", "init", "--directory", local],
        env,
    )
    write(local / "sync.txt", b"sync bytes produced by fd1338c")
    (local / "nested").mkdir()
    write(local / "nested" / "empty", b"")
    run([binary, "--volume", config, "sync", "run"], env)
    in_sync = json.loads(
        run(
            [binary, "--volume", config, "sync", "status", "--json"],
            env,
        ).stdout
    )
    if in_sync["summary"]["in_sync"] is not True:
        fail(f"fd1338c sync fixture did not converge: {in_sync}")
    backup = work / "backup"
    run(
        [
            binary,
            "--volume",
            config,
            "sync",
            "backup",
            "--output",
            backup,
        ],
        env,
    )
    write(local / "sync.txt", b"fd1338c local edit after baseline")
    pending = json.loads(
        run(
            [binary, "--volume", config, "sync", "status", "--json"],
            env,
        ).stdout
    )
    if pending["summary"]["in_sync"] is not False:
        fail(f"fd1338c pending sync fixture did not report a local change: {pending}")

    destination = output / "sync"
    keys = s3.copy_prefix(f"{root}/", destination / "storage")
    copy_sqlite_state(
        state,
        destination / "state",
        ["replica.db", "sync.db", "content/staging.db"],
    )
    shutil.copytree(local, destination / "local")
    shutil.copytree(backup, destination / "backup")
    write_config(
        destination / "sync-volume.json",
        "compat-sync",
        "sync",
        "library",
        "object",
        destination / "state",
    )
    expected = {
        "storage_keys": [key.removeprefix(f"{root}/") for key in keys],
        "local": local_tree(local),
        "in_sync_status": in_sync,
        "pending_status": pending,
        "head": head_identity(destination / "storage" / ".yinyang/head"),
        "filesystem": sqlite_value(
            destination / "state/replica.db",
            "SELECT hex(filesystem) FROM binding WHERE singleton=1",
        ),
    }
    write(destination / "expected.json", json.dumps(expected, indent=2).encode())


def sqlite_value(database, query):
    connection = sqlite3.connect(f"file:{database}?mode=ro&immutable=1", uri=True)
    try:
        row = connection.execute(query).fetchone()
        if row is None:
            fail(f"SQLite query returned no row in {database}: {query}")
        return row[0]
    finally:
        connection.close()


def manifest(output, producer, binary):
    files = []
    for path in sorted(output.rglob("*")):
        if path.name.endswith(("-wal", "-shm")):
            fail(f"quiesced fixture unexpectedly contains SQLite sidecar {path}")
        if not path.is_file() or path.name == "manifest.json":
            continue
        files.append(
            {
                "path": path.relative_to(output).as_posix(),
                "length": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    document = {
        "schema": SCHEMA,
        "producer": {
            "commit": producer,
            "binary_sha256": sha256_file(binary),
        },
        "fixture": {
            "object": {
                "commits": [OBJECT_COMMIT_V1, OBJECT_COMMIT_V2, OBJECT_COMMIT_JOURNAL],
                "state_handle": "stage_pending.handle",
            },
            "service": {"publication": "authenticated-loopback-service"},
            "sync": {"initial_commit": SYNC_INITIAL_COMMIT},
        },
        "files": files,
    }
    write(output / "manifest.json", json.dumps(document, indent=2).encode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--legacy-binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--producer", required=True)
    args = parser.parse_args()
    if args.producer != EXPECTED_PRODUCER:
        fail(
            f"fixture producer must be {EXPECTED_PRODUCER}, received {args.producer}"
        )
    if not args.legacy_binary.is_file():
        fail(f"legacy binary does not exist: {args.legacy_binary}")
    if args.output.exists() and any(args.output.iterdir()):
        fail(f"fixture output must be absent or empty: {args.output}")
    args.output.mkdir(parents=True, exist_ok=True)
    s3 = S3()
    with tempfile.TemporaryDirectory(prefix="yinyang-compatibility-") as temporary:
        temp = Path(temporary)
        create_object_fixture(args.legacy_binary, args.output, s3, temp)
        create_service_fixture(args.legacy_binary, args.output, s3, temp)
        create_sync_fixture(args.legacy_binary, args.output, s3, temp)
    manifest(args.output, args.producer, args.legacy_binary)
    print(f"compatibility fixtures written to {args.output}")


if __name__ == "__main__":
    main()
