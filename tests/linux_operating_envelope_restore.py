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

"""Verify retained remote bytes without writing new file pages.

The failed operating-envelope run can leave a persisted error ledger without
active handles. This helper reads one STS Credentials JSON document from stdin,
inspects the retained Managed state, restores the latest remote snapshot into a
new directory, and compares it with the deterministic dataset recorded by the
failed run. It never opens the failed Mount for writing and never calls pwrite.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

CREDENTIAL_NAMES = ("AccessKeyId", "SecretAccessKey", "SessionToken")
SCHEMA = "yinyang.linux-operating-envelope-recovery.v1"
PAGE_BYTES = 4096


class RecoveryError(RuntimeError):
    pass


def read_credentials() -> dict[str, str]:
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        raise RecoveryError(f"invalid STS credentials JSON: {error}") from error
    if isinstance(document, dict) and "Credentials" in document:
        document = document["Credentials"]
    if not isinstance(document, dict):
        raise RecoveryError("STS credentials JSON must be an object")
    values = {}
    for name in CREDENTIAL_NAMES:
        value = document.get(name)
        if not isinstance(value, str) or not value:
            raise RecoveryError(f"STS credentials JSON is missing {name}")
        values[name] = value
    return values


def redact(text: str, credentials: dict[str, str]) -> str:
    for value in credentials.values():
        text = text.replace(value, "<redacted>")
    return text


def run(
    command: list[str],
    env: dict[str, str],
    credentials: dict[str, str],
    timeout: float = 300,
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    stdout = redact(completed.stdout, credentials)
    stderr = redact(completed.stderr, credentials)
    if completed.returncode != 0:
        raise RecoveryError(
            f"command failed with {completed.returncode}: {command[0]}\n"
            f"stdout:\n{stdout}\nstderr:\n{stderr}"
        )
    return {
        "command": command,
        "stdout": stdout,
        "stderr": stderr,
    }


def storage_env(
    credentials: dict[str, str], bucket: str, region: str, root: str
) -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("YINYANG_")
        and name
        not in {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
        }
    }
    env.update(
        {
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "AWS_SESSION_TOKEN": credentials["SessionToken"],
            "AWS_REGION": region,
            "AWS_DEFAULT_REGION": region,
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_PAGER": "",
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
            "YINYANG_S3_BUCKET": bucket,
            "YINYANG_S3_REGION": region,
            "YINYANG_S3_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "YINYANG_S3_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "YINYANG_S3_SESSION_TOKEN": credentials["SessionToken"],
            "YINYANG_S3_ROOT": root,
        }
    )
    return env


def binary_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_dataset(failed: dict[str, Any]) -> tuple[int, int, int]:
    dataset = failed.get("dataset")
    if not isinstance(dataset, dict):
        raise RecoveryError("failed report has no dataset")
    files = dataset.get("files")
    file_bytes = dataset.get("file_bytes")
    seed = dataset.get("seed")
    for name, value in (("files", files), ("file_bytes", file_bytes), ("seed", seed)):
        if not isinstance(value, int) or value <= 0:
            raise RecoveryError(f"failed report has invalid dataset.{name}")
    if file_bytes % PAGE_BYTES != 0:
        raise RecoveryError("failed report file_bytes is not page aligned")
    return files, file_bytes, seed


def expected_files(failed: dict[str, Any]) -> dict[str, bytearray]:
    files, file_bytes, seed = read_dataset(failed)
    return {
        f"file-{index:04d}.bin": bytearray(
            random.Random(seed + index).randbytes(file_bytes)
        )
        for index in range(files)
    }


def sqlite_snapshot(state: Path) -> dict[str, Any]:
    database = state / "staging.db"
    if not database.is_file():
        return {"present": False}
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        result: dict[str, Any] = {
            "present": True,
            "tables": sorted(tables),
            "integrity_check": connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0],
        }
        if "handles" in tables:
            result["handles"] = connection.execute(
                "SELECT count(*) FROM handles"
            ).fetchone()[0]
        if "chunks" in tables:
            count, total = connection.execute(
                "SELECT count(*), coalesce(sum(length(data)), 0) FROM chunks"
            ).fetchone()
            result["chunks"] = {"count": count, "bytes": total}
        if "errors" in tables:
            result["errors"] = [
                {
                    "sequence": sequence,
                    "handle": handle,
                    "message": message,
                }
                for sequence, handle, message in connection.execute(
                    "SELECT sequence, hex(handle), message FROM errors "
                    "ORDER BY sequence"
                )
            ]
        return result
    finally:
        connection.close()


def file_digest(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"size": path.stat().st_size, "sha256": digest.hexdigest()}


def compare_files(root: Path, expected: dict[str, bytearray]) -> dict[str, Any]:
    actual = {entry.name for entry in root.iterdir() if entry.is_file()}
    expected_names = set(expected)
    if actual != expected_names:
        raise RecoveryError(
            f"restored path set differs: missing={sorted(expected_names - actual)}, "
            f"extra={sorted(actual - expected_names)}"
        )
    files = {}
    aggregate = hashlib.sha256()
    for name in sorted(expected):
        bytes_ = (root / name).read_bytes()
        if bytes_ != expected[name]:
            raise RecoveryError(f"restored bytes differ for {name}")
        digest = hashlib.sha256(bytes_).hexdigest()
        files[name] = {"size": len(bytes_), "sha256": digest}
        aggregate.update(name.encode())
        aggregate.update(bytes_)
    return {"files": files, "sha256": aggregate.hexdigest()}


def handle_count(status: str) -> int:
    match = re.search(r"local handles:\s*(\d+)", status)
    if match is None:
        raise RecoveryError("yy status did not report local handles")
    return int(match.group(1))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--yy", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--failed-json", type=Path, required=True)
    parser.add_argument("--storage-root", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.output.exists():
        raise RecoveryError(f"output already exists: {args.output}")
    failed = json.loads(args.failed_json.read_text())
    if failed.get("source_sha") is None:
        raise RecoveryError("failed report has no source_sha")
    expected = expected_files(failed)
    credentials = read_credentials()
    env = storage_env(credentials, args.bucket, args.region, args.storage_root)

    before = sqlite_snapshot(args.state)
    status_before = run(
        [str(args.yy), "--volume", str(args.config), "status"],
        env,
        credentials,
    )
    handles_before = handle_count(status_before["stdout"])

    work = args.output.parent / f".{args.output.name}.work"
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    restore_root = work / "restore"
    restore = run(
        [str(args.yy), "restore", str(restore_root)],
        env,
        credentials,
        timeout=900,
    )
    restored = compare_files(restore_root, expected)
    after = sqlite_snapshot(args.state)

    status = "remote-bytes-only" if handles_before == 0 else "recovery-required"
    result = {
        "schema": SCHEMA,
        "status": status,
        "source_sha": failed["source_sha"],
        "binary_sha256": binary_sha256(args.yy),
        "storage_root": args.storage_root,
        "local_handles_before": handles_before,
        "local_handles_after": after.get("handles"),
        "same_request_recovery": False,
        "remote_bytes_verified": True,
        "expected_files": len(expected),
        "restored": restored,
        "state_before": before,
        "state_after": after,
        "status_before": status_before,
        "restore": restore,
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    args.output.chmod(0o600)
    print(json.dumps({
        "schema": SCHEMA,
        "status": result["status"],
        "source_sha": result["source_sha"],
        "binary_sha256": result["binary_sha256"],
        "local_handles_before": handles_before,
        "remote_bytes_verified": True,
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RecoveryError, subprocess.SubprocessError) as error:
        print(f"restore verification failed: {error}", file=sys.stderr)
        sys.exit(1)
