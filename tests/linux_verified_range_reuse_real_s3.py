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

"""Run the real Amazon S3 repeated/random range benchmark.

Invoke through the shared Linux validation runner with STS credentials on
stdin. Credentials never appear in arguments, files or process output.
"""

import json
import os
import re
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


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def main() -> int:
    credentials = credentials_from_stdin()
    bucket = required_environment("YINYANG_BENCH_BUCKET")
    root = required_environment("YINYANG_BENCH_ROOT")
    output = required_environment("YINYANG_BENCH_OUTPUT")
    label = required_environment("YINYANG_BENCH_LABEL")
    range_policy = required_environment("YINYANG_BENCH_RANGE_POLICY")
    if not re.fullmatch(r"[a-z0-9-]+", label):
        raise SystemExit("YINYANG_BENCH_LABEL must contain lowercase letters, digits and hyphens")
    region = os.environ.get("YINYANG_QUALIFICATION_REGION", "us-east-2")

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
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
            "YINYANG_S3_BUCKET": bucket,
            "YINYANG_S3_REGION": region,
            "YINYANG_S3_ROOT": root,
            "YINYANG_S3_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "YINYANG_S3_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "YINYANG_S3_SESSION_TOKEN": credentials["SessionToken"],
            "YINYANG_COST_PROFILE": "full",
            "YINYANG_COST_WORKLOAD": "repeated-random",
            "YINYANG_COST_PUBLICATION": "object",
            "YINYANG_COST_OUTPUT": output,
            "YINYANG_COST_SUMMARY": os.path.splitext(output)[0] + ".md",
            "YINYANG_COST_SOURCE_SHA": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            "YINYANG_COST_SOURCE_DIRTY": (
                "1"
                if subprocess.check_output(
                    ["git", "status", "--porcelain"], text=True
                ).strip()
                else "0"
            ),
            "YINYANG_COST_STORAGE_LABEL": (
                f"Amazon S3 {region} over HTTPS; role-scoped non-loopback fixture"
            ),
            "YINYANG_COST_RANGE_POLICY": range_policy,
        }
    )

    command = [
        "cargo",
        "test",
        "--locked",
        "--release",
        "-p",
        "yinyang",
        "--features",
        "benchmark-telemetry",
        "--test",
        "linux_cost",
        "linux_cost_benchmark",
        "--",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ]
    print(
        "real Amazon S3 repeated/random benchmark: "
        f"source={inherited['YINYANG_COST_SOURCE_SHA']} "
        f"dirty={inherited['YINYANG_COST_SOURCE_DIRTY']} "
        f"region={region} bucket={bucket} root={root} label={label} "
        f"output={output}",
        flush=True,
    )
    completed = subprocess.run(
        command,
        env=inherited,
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    rendered = completed.stdout
    for value in credentials.values():
        rendered = rendered.replace(value, "<redacted>")
    print(rendered, end="", flush=True)
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
