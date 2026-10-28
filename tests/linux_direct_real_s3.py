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

The helper removes every inherited YINYANG_* and AWS endpoint setting before
starting the tests. It never prints, writes or places credentials in command
arguments.
"""

import json
import os
import shutil
import subprocess
import sys


CREDENTIAL_NAMES = ("AccessKeyId", "SecretAccessKey", "SessionToken")


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
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
        }
    )

    source = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    print(
        "real Amazon S3 Direct qualification: "
        f"source={source} region={region} "
        f"unversioned={unversioned} versioned={versioned} "
        f"aws_cli={aws_cli}",
        flush=True,
    )
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "-p",
            "yinyang",
            "--test",
            "linux_direct",
            "direct_amazon_s3_",
            "--",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ],
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
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
