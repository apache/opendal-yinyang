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

"""Run the native Amazon S3 smoke with STS credentials from stdin.

Invoke this helper as the command of the shared Linux validation runner. The
runner has already acquired its global slot and installed its default MinIO
environment. This helper replaces only the YINYANG_* child environment and
never prints, writes or places credentials in command arguments.

stdin:
    AWS STS Credentials JSON, either the Credentials object itself or a
    wrapping {"Credentials": {...}} document, with AccessKeyId,
    SecretAccessKey and SessionToken.

environment:
    YINYANG_QUALIFICATION_BUCKET
    YINYANG_QUALIFICATION_REGION (optional, defaults to us-east-2)

action:
    cargo test --test linux_operations real_amazon_s3_cli_smoke
        -- --ignored --nocapture
"""

import json
import os
import subprocess
import sys
import uuid


def credentials_from_stdin() -> dict:
    try:
        document = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError) as error:
        raise SystemExit(f"invalid STS credentials JSON: {error.__class__.__name__}")
    if isinstance(document, dict) and "Credentials" in document:
        document = document["Credentials"]
    if not isinstance(document, dict):
        raise SystemExit("STS credentials JSON must be an object")
    values = {}
    for name in ("AccessKeyId", "SecretAccessKey", "SessionToken"):
        value = document.get(name)
        if not isinstance(value, str) or not value:
            raise SystemExit(f"STS credentials JSON is missing {name}")
        values[name] = value
    return values


def main() -> int:
    credentials = credentials_from_stdin()
    bucket = os.environ.get("YINYANG_QUALIFICATION_BUCKET")
    if not bucket:
        raise SystemExit("YINYANG_QUALIFICATION_BUCKET is required")
    region = os.environ.get("YINYANG_QUALIFICATION_REGION", "us-east-2")

    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("YINYANG_")
    }
    environment.update(
        {
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
            "YINYANG_S3_BUCKET": bucket,
            "YINYANG_S3_REGION": region,
            "YINYANG_S3_ROOT": f"linux-operations/amazon-s3-{uuid.uuid4().hex}",
            "YINYANG_S3_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "YINYANG_S3_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "YINYANG_S3_SESSION_TOKEN": credentials["SessionToken"],
        }
    )
    print(
        f"real Amazon S3 exact-CLI smoke: bucket={bucket} region={region} "
        f"source={subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}",
        flush=True,
    )
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "--test",
            "linux_operations",
            "real_amazon_s3_cli_smoke",
            "--",
            "--ignored",
            "--nocapture",
        ],
        env=environment,
        check=False,
    )
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
