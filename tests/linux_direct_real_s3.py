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

"""Run Amazon S3 Direct qualification with STS credentials from stdin.

Invoke this helper through the shared Linux validation runner:

    python3 tests/linux_direct_real_s3.py

stdin:
    AWS STS Credentials JSON, either the Credentials object itself or a
    wrapping {"Credentials": {...}} document, with AccessKeyId,
    SecretAccessKey and SessionToken.

environment:
    YINYANG_DIRECT_UNVERSIONED_BUCKET
    YINYANG_DIRECT_VERSIONED_BUCKET
    YINYANG_QUALIFICATION_REGION (optional, defaults to us-east-2)
    AWS_CLI_BIN (optional, defaults to aws)

argument:
    optional test-name filter beginning with direct_amazon_s3_, or
    --conditional-entrypoints for the existing writable remount and finite
    Direct Sync entrypoint smoke

The helper removes every inherited YINYANG_* and AWS endpoint setting before
starting the tests. It never prints, writes or places credentials in command
arguments.
"""

import json
import os
import shutil
import subprocess
import sys
from glob import glob
from hashlib import sha256


CREDENTIAL_NAMES = ("AccessKeyId", "SecretAccessKey", "SessionToken")
ENTRYPOINT_TESTS = (
    "writable::direct_write_mount_publishes_whole_objects_and_persists_remount",
    "sync::direct_sync_finite_reconciliation_tracks_both_authorities",
)


def credentials_from_stdin() -> dict[str, str]:
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        raise SystemExit(f"invalid STS credentials JSON: {error.__class__.__name__}")
    if isinstance(document, dict) and "Credentials" in document:
        document = document["Credentials"]
    if not isinstance(document, dict):
        raise SystemExit("STS credentials JSON must be an object")
    values = {}
    for name in CREDENTIAL_NAMES:
        value = document.get(name)
        if not isinstance(value, str) or not value:
            raise SystemExit(f"STS credentials JSON is missing {name}")
        values[name] = value
    return values


def main() -> int:
    credentials = credentials_from_stdin()
    if len(sys.argv) > 2:
        raise SystemExit("at most one Direct test-name filter is accepted")
    argument = sys.argv[1] if len(sys.argv) == 2 else "direct_amazon_s3_"
    entrypoint_smoke = argument == "--conditional-entrypoints"
    test_filter = argument if not entrypoint_smoke else None
    if test_filter is not None and not test_filter.startswith("direct_amazon_s3_"):
        raise SystemExit("Direct test-name filter must begin with direct_amazon_s3_")
    unversioned = os.environ.get("YINYANG_DIRECT_UNVERSIONED_BUCKET")
    versioned = os.environ.get("YINYANG_DIRECT_VERSIONED_BUCKET")
    if not unversioned:
        raise SystemExit("YINYANG_DIRECT_UNVERSIONED_BUCKET is required")
    if not versioned:
        raise SystemExit("YINYANG_DIRECT_VERSIONED_BUCKET is required")
    if unversioned == versioned:
        raise SystemExit("versioned and unversioned buckets must be distinct")
    region = os.environ.get("YINYANG_QUALIFICATION_REGION", "us-east-2")
    aws_cli = os.environ.get("AWS_CLI_BIN", "aws")
    if shutil.which(aws_cli) is None:
        raise SystemExit(f"AWS CLI is not executable: {aws_cli}")
    aws_version = subprocess.check_output(
        [aws_cli, "--version"], stderr=subprocess.STDOUT, text=True
    ).strip()

    inherited = {
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
    inherited.update(
        {
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "AWS_SESSION_TOKEN": credentials["SessionToken"],
            "AWS_REGION": region,
            "AWS_DEFAULT_REGION": region,
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CLI_BIN": aws_cli,
            "YINYANG_DIRECT_UNVERSIONED_BUCKET": unversioned,
            "YINYANG_DIRECT_VERSIONED_BUCKET": versioned,
            "YINYANG_QUALIFICATION_REGION": region,
            "YINYANG_S3_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "YINYANG_S3_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "YINYANG_S3_SESSION_TOKEN": credentials["SessionToken"],
            "YINYANG_QUALIFICATION_REGION": region,
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
            "AWS_CLI_BIN": aws_cli,
        }
    )
    if entrypoint_smoke:
        inherited.update(
            {
                "YINYANG_S3_BUCKET": unversioned,
                "YINYANG_S3_REGION": region,
                "YINYANG_DIRECT_TEST_STORAGE_PROFILE": "amazon-s3",
            }
        )

    source = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    print(
        "real Amazon S3 Direct qualification: "
        f"source={source} region={region} "
        f"unversioned={unversioned} versioned={versioned} "
        f"aws_cli={aws_cli} aws_version={aws_version} "
        f"mode={'entrypoints' if entrypoint_smoke else 'qualification'}",
        flush=True,
    )
    tests = ENTRYPOINT_TESTS if entrypoint_smoke else (test_filter,)
    for test in tests:
        command = [
            "cargo",
            "test",
            "--locked",
            "-p",
            "yinyang",
            "--test",
            "linux_direct",
            test,
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]
        if entrypoint_smoke:
            command.insert(-3, "--exact")
        completed = subprocess.run(
            command,
            env=inherited,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = completed.stdout
        for value in credentials.values():
            output = output.replace(value, "<redacted>")
        print(output, end="", flush=True)
        if completed.returncode != 0:
            return completed.returncode

    target = os.environ.get("CARGO_TARGET_DIR", "target")
    yy = os.path.join(target, "debug", "yy")
    if os.path.isfile(yy):
        print(f"yy sha256={file_sha256(yy)}", flush=True)
    candidates = [
        path
        for path in glob(os.path.join(target, "debug", "deps", "linux_direct-*"))
        if os.path.isfile(path) and os.access(path, os.X_OK)
    ]
    if candidates:
        newest = max(candidates, key=os.path.getmtime)
        print(f"linux_direct sha256={file_sha256(newest)}", flush=True)
    return 0


def file_sha256(path: str) -> str:
    digest = sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    sys.exit(main())
