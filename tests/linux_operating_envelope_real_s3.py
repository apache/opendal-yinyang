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

"""Run the Linux operating-envelope workload with STS credentials from stdin.

Invoke this helper through the shared Linux validation runner. It removes
inherited credentials and endpoint overrides, installs only one short-lived
identity in child environments, and never writes credential values to disk or
arguments.

stdin:
    AWS STS Credentials JSON, either the Credentials object itself or a
    wrapping {"Credentials": {...}} document.

environment:
    YINYANG_OPERATING_ENVELOPE_BUCKET
    YINYANG_OPERATING_ENVELOPE_YY (defaults to target/release/yy)
    YINYANG_OPERATING_ENVELOPE_OUTPUT
    YINYANG_OPERATING_ENVELOPE_DURATION (defaults to 1800)
    YINYANG_OPERATING_ENVELOPE_REQUEST_ACCOUNTING (optional)
    YINYANG_QUALIFICATION_REGION (optional, defaults to us-east-2)
    AWS_CLI_BIN (optional, defaults to aws)
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

CREDENTIAL_NAMES = ("AccessKeyId", "SecretAccessKey", "SessionToken")
DEFAULT_FILES = 8
DEFAULT_FILE_BYTES = 8 * 1024 * 1024 + 17
DEFAULT_WRITERS = 4
DEFAULT_READERS = 2


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


def read_artifact_manifest(yy: Path) -> dict[str, str]:
    candidates = (
        yy.resolve().parent / "BUILD-MANIFEST",
        yy.resolve().parent.parent / "BUILD-MANIFEST",
    )
    manifest_path = next(
        (candidate for candidate in candidates if candidate.is_file()),
        None,
    )
    if manifest_path is None:
        raise SystemExit(
            "measured yy must be inside an installed archive with BUILD-MANIFEST"
        )
    fields = {}
    for line in manifest_path.read_text().splitlines():
        name, separator, value = line.partition("=")
        if separator:
            fields[name] = value
    required = ("schema", "source_commit", "target", "yy_sha256")
    for name in required:
        if not fields.get(name):
            raise SystemExit(f"BUILD-MANIFEST is missing {name}")
    if fields["schema"] != "yinyang-linux-release-artifact.v1":
        raise SystemExit(f"unexpected BUILD-MANIFEST schema: {fields['schema']}")
    digest = hashlib.sha256()
    with yy.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != fields["yy_sha256"]:
        raise SystemExit("measured yy SHA-256 differs from BUILD-MANIFEST")
    return fields


def main() -> int:
    credentials = credentials_from_stdin()
    bucket = os.environ.get("YINYANG_OPERATING_ENVELOPE_BUCKET")
    if not bucket:
        raise SystemExit("YINYANG_OPERATING_ENVELOPE_BUCKET is required")
    region = os.environ.get("YINYANG_QUALIFICATION_REGION", "us-east-2")
    yy = os.environ.get("YINYANG_OPERATING_ENVELOPE_YY", "target/release/yy")
    output = os.environ.get("YINYANG_OPERATING_ENVELOPE_OUTPUT")
    if not output:
        raise SystemExit("YINYANG_OPERATING_ENVELOPE_OUTPUT is required")
    duration = os.environ.get("YINYANG_OPERATING_ENVELOPE_DURATION", "1800")
    files = os.environ.get("YINYANG_OPERATING_ENVELOPE_FILES", str(DEFAULT_FILES))
    file_bytes = os.environ.get(
        "YINYANG_OPERATING_ENVELOPE_FILE_BYTES", str(DEFAULT_FILE_BYTES)
    )
    writers = os.environ.get(
        "YINYANG_OPERATING_ENVELOPE_WRITERS", str(DEFAULT_WRITERS)
    )
    readers = os.environ.get(
        "YINYANG_OPERATING_ENVELOPE_READERS", str(DEFAULT_READERS)
    )
    aws_cli = os.environ.get("AWS_CLI_BIN", "aws")
    if shutil.which(aws_cli) is None:
        raise SystemExit(f"AWS CLI is not executable: {aws_cli}")
    if not Path(yy).is_file():
        raise SystemExit(f"yy binary is unavailable: {yy}")
    artifact_manifest = read_artifact_manifest(Path(yy))
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
            "AWS_PAGER": "",
            "AWS_CLI_BIN": aws_cli,
            "YINYANG_STORAGE_PROFILE": "amazon-s3",
            "YINYANG_S3_BUCKET": bucket,
            "YINYANG_S3_REGION": region,
            "YINYANG_S3_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "YINYANG_S3_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "YINYANG_S3_SESSION_TOKEN": credentials["SessionToken"],
        }
    )
    script_source = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()
    artifact_source = artifact_manifest["source_commit"]
    command = [
        sys.executable,
        "tests/linux_operating_envelope.py",
        "--yy",
        yy,
        "--source-sha",
        artifact_source,
        "--script-source-sha",
        script_source,
        "--artifact-provenance",
        "installed-archive-BUILD-MANIFEST",
        "--backend-mode",
        "amazon-s3",
        "--backend-label",
        f"amazon-s3-{region}",
        "--output",
        output,
        "--duration-seconds",
        duration,
        "--files",
        files,
        "--file-bytes",
        file_bytes,
        "--writers",
        writers,
        "--readers",
        readers,
    ]
    request_accounting = os.environ.get("YINYANG_OPERATING_ENVELOPE_REQUEST_ACCOUNTING")
    if request_accounting:
        command.extend(["--request-accounting", request_accounting])
    print(
        "real Amazon S3 operating envelope: "
        f"artifact_source={artifact_source} script_source={script_source} "
        f"region={region} bucket={bucket} duration={duration}s "
        f"files={files} file_bytes={file_bytes} writers={writers} "
        f"readers={readers} aws_cli={aws_cli} aws_version={aws_version}",
        flush=True,
    )
    completed = subprocess.run(
        command,
        env=inherited,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    output_text = completed.stdout
    for value in credentials.values():
        output_text = output_text.replace(value, "<redacted>")
    print(output_text, end="", flush=True)
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
