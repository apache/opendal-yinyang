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

"""Exercise one installed target binary through FUSE on Amazon S3.

Invoke this helper through the shared Linux validation runner. STS credentials
are read only from stdin and installed in child environments; they are never
written to disk or command arguments.

stdin:
    AWS STS Credentials JSON, either the Credentials object itself or a
    wrapping {"Credentials": {...}} document.

environment:
    YINYANG_QUALIFICATION_BUCKET
    YINYANG_QUALIFICATION_REGION (optional, defaults to us-east-2)
    YINYANG_STAGE3_TARGET_YY (installed archive binary)
    YY_UPGRADE_RELEASE (optional)
    YINYANG_STAGE3_REAL_S3_OUTPUT (optional JSON evidence path)
    AWS_CLI_BIN (optional, defaults to aws)

The workload is bounded to one object authority and one multi-leaf FUSE file.
It checks append, truncate, zero extension, shrink-then-extend, clean remount,
independent remote restores at the append and final stages, installed
`space report` and `doctor`, and exact-prefix cleanup.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from linux_operating_envelope import (  # noqa: E402
    EnvelopeError,
    MountProcess,
    binary_sha256,
    bytes_sha256,
    utc_now,
)

CREDENTIAL_NAMES = ("AccessKeyId", "SecretAccessKey", "SessionToken")
SCHEMA = "yinyang.linux-stage3-real-s3-trial.v1"
INITIAL_BYTES = 8 * 1024 * 1024 + 17
APPEND_BYTES = b"stage3-real-s3-append".ljust(4 * 1024, b".")
ZERO_EXTEND_BYTES = 32 * 1024
SHRINK_BYTES = 4096
EXTEND_BYTES = 64 * 1024 + 4096
REMOUNT_APPEND_BYTES = b"remount-appended"


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


def payload(length: int, seed: int) -> bytes:
    return bytes((seed + index * 17 + index // 251) % 256 for index in range(length))


def write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise EnvelopeError("short write to a private trial file")
        view = view[written:]


class Trial:
    def __init__(self, credentials: dict[str, str]) -> None:
        self.credentials = credentials
        self.bucket = os.environ.get("YINYANG_QUALIFICATION_BUCKET")
        if not self.bucket:
            raise EnvelopeError("YINYANG_QUALIFICATION_BUCKET is required")
        self.region = os.environ.get("YINYANG_QUALIFICATION_REGION", "us-east-2")
        try:
            self.seed = int(os.environ.get("YINYANG_STAGE3_SEED", "9014"))
        except ValueError as error:
            raise EnvelopeError("YINYANG_STAGE3_SEED must be an integer") from error
        self.yy = Path(os.environ.get("YINYANG_STAGE3_TARGET_YY", ""))
        if not self.yy.is_file():
            raise EnvelopeError(
                "YINYANG_STAGE3_TARGET_YY must name the installed archive binary"
            )
        self.release = os.environ.get("YY_UPGRADE_RELEASE")
        self.output = (
            Path(os.environ["YINYANG_STAGE3_REAL_S3_OUTPUT"]).resolve()
            if os.environ.get("YINYANG_STAGE3_REAL_S3_OUTPUT")
            else None
        )
        self.aws_cli = os.environ.get("AWS_CLI_BIN", "aws")
        self.source = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        self.archive_manifest = self.read_archive_manifest()
        self.work = Path(
            tempfile.mkdtemp(prefix="yinyang-stage3-real-s3-", dir="/var/tmp")
        )
        self.state = self.work / "state"
        self.config_dir = self.work / "config"
        self.mountpoint = self.work / "mount"
        self.logs = self.work / "logs"
        for directory in (
            self.state,
            self.config_dir,
            self.mountpoint,
            self.logs,
        ):
            directory.mkdir(mode=0o700)
        self.config = self.config_dir / "stage3.json"
        self.root = f"stage3-real-s3/{uuid.uuid4().hex}"
        self.secrets = list(credentials.values())
        self.env = self.storage_environment()
        self.aws_env = self.aws_environment()
        self.mount = MountProcess(
            self.yy, self.env, self.config, self.mountpoint, self.logs
        )
        self.started = time.monotonic()
        self.report: dict[str, Any] = {
            "schema": SCHEMA,
            "status": "running",
            "started_at": utc_now(),
            "source_checkout": self.source,
            "seed": self.seed,
            "target": {
                "binary": str(self.yy),
                "binary_sha256": binary_sha256(self.yy),
                "release": self.archive_manifest["release"],
                "archive_source_commit": self.archive_manifest["source_commit"],
                "archive_yy_sha256": self.archive_manifest["yy_sha256"],
                "archive_target": self.archive_manifest["target"],
                "provenance": "installed-archive-build-manifest",
                "source_matches_checkout": (
                    self.archive_manifest["source_commit"] == self.source
                ),
            },
            "backend": {
                "profile": "amazon-s3",
                "bucket": self.bucket,
                "region": self.region,
                "storage_root": self.root,
                "versioning": "not-required-unverified",
            },
            "host": {
                "machine": platform.machine(),
                "platform": platform.platform(),
                "kernel": platform.release(),
            },
        }

    def read_archive_manifest(self) -> dict[str, str]:
        release_dir = self.yy.resolve().parent.parent
        manifest_path = release_dir / "BUILD-MANIFEST"
        if not manifest_path.is_file():
            raise EnvelopeError(
                "YINYANG_STAGE3_TARGET_YY must be inside an installed archive "
                "release with BUILD-MANIFEST"
            )
        fields = {}
        for line in manifest_path.read_text().splitlines():
            name, separator, value = line.partition("=")
            if separator:
                fields[name] = value
        for name in (
            "schema",
            "version",
            "release",
            "source_commit",
            "target",
            "yy_sha256",
        ):
            if not fields.get(name):
                raise EnvelopeError(f"BUILD-MANIFEST is missing {name}")
        if fields["schema"] != "yinyang-linux-release-artifact.v1":
            raise EnvelopeError(
                f"unexpected BUILD-MANIFEST schema: {fields['schema']}"
            )
        if fields["target"] != "ubuntu-24.04-x86_64":
            raise EnvelopeError(
                f"unexpected BUILD-MANIFEST target: {fields['target']}"
            )
        actual_sha256 = binary_sha256(self.yy)
        if fields["yy_sha256"] != actual_sha256:
            raise EnvelopeError(
                "installed archive binary SHA-256 differs from BUILD-MANIFEST"
            )
        if self.release and self.release != fields["release"]:
            raise EnvelopeError(
                "YY_UPGRADE_RELEASE differs from the installed BUILD-MANIFEST release"
            )
        return fields

    def redact(self, text: str) -> str:
        output = text
        for secret in self.secrets:
            output = output.replace(secret, "<redacted>")
        return output

    def storage_environment(self) -> dict[str, str]:
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
                "YINYANG_STORAGE_PROFILE": "amazon-s3",
                "YINYANG_S3_BUCKET": self.bucket,
                "YINYANG_S3_REGION": self.region,
                "YINYANG_S3_ROOT": self.root,
                "YINYANG_S3_ACCESS_KEY_ID": self.credentials["AccessKeyId"],
                "YINYANG_S3_SECRET_ACCESS_KEY": self.credentials["SecretAccessKey"],
                "YINYANG_S3_SESSION_TOKEN": self.credentials["SessionToken"],
                "AWS_EC2_METADATA_DISABLED": "true",
            }
        )
        return env

    def aws_environment(self) -> dict[str, str]:
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
                "AWS_ACCESS_KEY_ID": self.credentials["AccessKeyId"],
                "AWS_SECRET_ACCESS_KEY": self.credentials["SecretAccessKey"],
                "AWS_SESSION_TOKEN": self.credentials["SessionToken"],
                "AWS_REGION": self.region,
                "AWS_DEFAULT_REGION": self.region,
                "AWS_CONFIG_FILE": os.devnull,
                "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_PAGER": "",
                "AWS_CLI_BIN": self.aws_cli,
            }
        )
        return env

    def run(
        self,
        command: list[str],
        *,
        check: bool = True,
        timeout: float = 180,
    ) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            command,
            env=self.env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        stdout = self.redact(completed.stdout)
        stderr = self.redact(completed.stderr)
        if check and completed.returncode != 0:
            raise EnvelopeError(
                f"command failed with {completed.returncode}: {command[0]}\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        return subprocess.CompletedProcess(
            completed.args, completed.returncode, stdout, stderr
        )

    def yy_run(
        self, arguments: list[str], *, timeout: float = 180
    ) -> subprocess.CompletedProcess[str]:
        return self.run([str(self.yy), *arguments], timeout=timeout)

    def aws(self, arguments: list[str], timeout: float = 120) -> dict[str, Any]:
        completed = subprocess.run(
            [
                self.aws_cli,
                "s3api",
                *arguments,
                "--region",
                self.region,
                "--no-cli-pager",
                "--output",
                "json",
            ],
            env=self.aws_env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        stdout = self.redact(completed.stdout)
        stderr = self.redact(completed.stderr)
        if completed.returncode != 0:
            raise EnvelopeError(
                f"AWS CLI failed with {completed.returncode}\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        if not stdout.strip():
            return {}
        try:
            return json.loads(stdout)
        except json.JSONDecodeError as error:
            raise EnvelopeError(f"AWS CLI returned invalid JSON: {error}") from error

    def preflight(self) -> None:
        if platform.system() != "Linux" or platform.machine() != "x86_64":
            raise EnvelopeError("the stage-3 real-S3 trial requires Linux x86_64")
        for command in ("fusermount3", self.aws_cli):
            if shutil.which(command) is None:
                raise EnvelopeError(f"required command is unavailable: {command}")
        if not Path("/dev/fuse").exists():
            raise EnvelopeError("/dev/fuse is unavailable")
        if os.environ.get("YINYANG_S3_ENDPOINT"):
            raise EnvelopeError(
                "YINYANG_S3_ENDPOINT must be unset for the real Amazon S3 trial"
            )

    def write_config(self) -> None:
        document = {
            "name": "stage3-real-s3",
            "model": "managed",
            "access": "mount",
            "frontend": "fuse",
            "publication": "object",
            "storage_profile": "amazon-s3",
            "staging": str(self.state),
            "read_only": False,
            "require": [],
        }
        descriptor = os.open(
            self.config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        try:
            write_all(descriptor, (json.dumps(document, indent=2) + "\n").encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def install_authority(self) -> None:
        self.write_config()
        self.yy_run(["create"], timeout=300)
        self.yy_run(["--volume", str(self.config), "capabilities"], timeout=300)

    def assert_mount_bytes(self, expected: bytes, context: str) -> None:
        actual = (self.mountpoint / "stage3.bin").read_bytes()
        if actual != expected:
            raise EnvelopeError(
                f"{context} bytes differ: expected {len(expected)}, got {len(actual)}"
            )

    def restore_remote(self, expected: bytes, label: str) -> dict[str, Any]:
        destination = self.work / f"restore-{label}-{uuid.uuid4().hex}"
        self.yy_run(["restore", str(destination)], timeout=300)
        actual = (destination / "stage3.bin").read_bytes()
        if actual != expected:
            raise EnvelopeError(
                f"{label} remote restore differs: expected {len(expected)}, got {len(actual)}"
            )
        return {
            "label": label,
            "length": len(actual),
            "sha256": bytes_sha256(actual),
        }

    def doctor(self) -> dict[str, Any]:
        completed = self.yy_run(
            ["--volume", str(self.config), "doctor", "--json"], timeout=180
        )
        document = json.loads(completed.stdout)
        if document.get("schema") != "yinyang.operational-health.v1":
            raise EnvelopeError(
                f"unexpected doctor schema: {document.get('schema')}"
            )
        if document.get("status") != "pass":
            raise EnvelopeError(f"installed target doctor did not pass: {document}")
        return {"schema": document["schema"], "status": document["status"]}

    def exercise_mount(self) -> dict[str, Any]:
        self.mount.start(self.redact)
        path = self.mountpoint / "stage3.bin"
        expected = payload(INITIAL_BYTES, self.seed)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            view = memoryview(expected)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise EnvelopeError("short initial write through FUSE")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self.assert_mount_bytes(expected, "initial close")
        initial_space = self.space_report(INITIAL_BYTES)

        with path.open("ab", buffering=0) as stream:
            written = stream.write(APPEND_BYTES)
            if written != len(APPEND_BYTES):
                raise EnvelopeError(
                    f"short FUSE append write: expected {len(APPEND_BYTES)}, got {written}"
                )
            os.fsync(stream.fileno())
        expected += APPEND_BYTES
        self.assert_mount_bytes(expected, "append after close")
        append_restore = self.restore_remote(expected, "append")

        os.truncate(path, len(expected) + ZERO_EXTEND_BYTES)
        expected += b"\0" * ZERO_EXTEND_BYTES
        self.assert_mount_bytes(expected, "zero extension")

        descriptor = os.open(path, os.O_RDWR)
        try:
            os.ftruncate(descriptor, SHRINK_BYTES)
            os.ftruncate(descriptor, EXTEND_BYTES)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        expected = expected[:SHRINK_BYTES] + b"\0" * (EXTEND_BYTES - SHRINK_BYTES)
        self.assert_mount_bytes(expected, "first mounted phase")
        clean_unmount = self.mount.stop(self.redact)
        return {
            "expected": expected,
            "initial_space": initial_space,
            "append_restore": append_restore,
            "first_clean_unmount": clean_unmount,
        }

    def exercise_remount(self, expected: bytes) -> dict[str, Any]:
        self.mount.start(self.redact)
        self.assert_mount_bytes(expected, "remount before append")
        with (self.mountpoint / "stage3.bin").open("ab", buffering=0) as stream:
            written = stream.write(REMOUNT_APPEND_BYTES)
            if written != len(REMOUNT_APPEND_BYTES):
                raise EnvelopeError(
                    "short remount append write: "
                    f"expected {len(REMOUNT_APPEND_BYTES)}, got {written}"
                )
            os.fsync(stream.fileno())
        expected += REMOUNT_APPEND_BYTES
        self.assert_mount_bytes(expected, "remount after append")
        return {
            "expected": expected,
        }

    def space_report(self, expected_length: int) -> dict[str, Any]:
        completed = self.yy_run(
            ["--volume", str(self.config), "space", "report", "--json"]
        )
        document = json.loads(completed.stdout)
        if document.get("schema") != "yinyang.managed-space.report.v3":
            raise EnvelopeError(
                f"unexpected managed-space report schema: {document.get('schema')}"
            )
        if document.get("deletion_authorized") is not False:
            raise EnvelopeError("space report authorized deletion")
        if document.get("reclaimable_bytes") is not None:
            raise EnvelopeError("space report invented reclaimable bytes")
        live = document["logical"]["live"]
        if live.get("files") != 1 or live.get("file_bytes") != expected_length:
            raise EnvelopeError(
                "space report logical live measurements differ from the FUSE bytes: "
                f"{live}"
            )
        retained = document["logical"]["retained"]
        physical = document["physical"]
        reachability = physical["reachability"]
        return {
            "schema": document["schema"],
            "observed_revision": document["observed_revision"],
            "seed": self.seed,
            "logical_live": {
                "files": live["files"],
                "file_bytes": live["file_bytes"],
                "unique_content_count": live["unique_content_count"],
                "unique_content_bytes": live["unique_content_bytes"],
            },
            "logical_retained": {
                "state": retained["state"],
                "revision_count": retained["revision_count"],
                "observed_revisions": len(retained["observed_revisions"]),
                "observed_file_bytes": retained["observed_file_bytes"],
                "observed_unique_content_bytes": retained[
                    "observed_unique_content_bytes"
                ],
            },
            "physical": {
                "state": physical["state"],
                "visible_objects": physical["visible_objects"],
                "visible_bytes": physical["visible_bytes"],
                "head": physical["head"],
                "packs": physical["packs"],
                "noncurrent_versions": physical["noncurrent_versions"],
                "reachability_state": reachability["state"],
                "retained_revisions": reachability["retained_revisions"],
                "content_extent_references": reachability[
                    "content_extent_references"
                ],
                "shared_packs": reachability["shared_packs"],
            },
            "deletion_authorized": document["deletion_authorized"],
            "reclaimable_bytes": document["reclaimable_bytes"],
        }

    @staticmethod
    def space_delta(
        before: dict[str, Any], after: dict[str, Any], path: tuple[str, ...]
    ) -> dict[str, Any]:
        before_value: Any = before
        after_value: Any = after
        for segment in path:
            before_value = before_value.get(segment)
            after_value = after_value.get(segment)
        delta = None
        if isinstance(before_value, int) and isinstance(after_value, int):
            delta = after_value - before_value
        return {"before": before_value, "after": after_value, "delta": delta}

    def cleanup_remote(self) -> dict[str, Any]:
        prefix = f"{self.root}/"
        keys: list[str] = []
        token: str | None = None
        while True:
            arguments = [
                "list-objects-v2",
                "--bucket",
                self.bucket,
                "--prefix",
                prefix,
            ]
            if token:
                arguments.extend(["--continuation-token", token])
            document = self.aws(arguments)
            keys.extend(item["Key"] for item in document.get("Contents", []))
            if not document.get("IsTruncated"):
                break
            token = document.get("NextContinuationToken")
            if not token:
                raise EnvelopeError(
                    "truncated ListObjectsV2 response omitted a continuation token"
                )
        for start in range(0, len(keys), 1000):
            batch = keys[start : start + 1000]
            self.aws(
                [
                    "delete-objects",
                    "--bucket",
                    self.bucket,
                    "--delete",
                    json.dumps({"Objects": [{"Key": key} for key in batch]}),
                ]
            )
        remaining = self.aws(
            [
                "list-objects-v2",
                "--bucket",
                self.bucket,
                "--prefix",
                prefix,
                "--max-keys",
                "1",
            ]
        )
        if remaining.get("Contents"):
            raise EnvelopeError("exact stage-3 prefix cleanup left objects behind")
        return {"listed_keys": len(keys), "remaining_keys": 0}

    def publish_report(self) -> None:
        if self.output is None:
            return
        self.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.output.with_name(f".{self.output.name}.{os.getpid()}.tmp")
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
        )
        try:
            write_all(
                descriptor,
                (json.dumps(self.report, indent=2, sort_keys=True) + "\n").encode(),
            )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self.output)

    def execute(self) -> dict[str, Any]:
        self.preflight()
        self.install_authority()
        first = self.exercise_mount()
        remount = self.exercise_remount(first["expected"])
        final_bytes = remount["expected"]
        final_space = self.space_report(len(final_bytes))
        initial_space = first["initial_space"]
        space_comparison = {
            "seed": self.seed,
            "visible_objects": self.space_delta(
                initial_space, final_space, ("physical", "visible_objects")
            ),
            "visible_bytes": self.space_delta(
                initial_space, final_space, ("physical", "visible_bytes")
            ),
            "retained_revisions": self.space_delta(
                initial_space, final_space, ("physical", "retained_revisions")
            ),
            "retained_observed_revisions": self.space_delta(
                initial_space, final_space, ("logical_retained", "observed_revisions")
            ),
            "retained_observed_file_bytes": self.space_delta(
                initial_space,
                final_space,
                ("logical_retained", "observed_file_bytes"),
            ),
            "retained_observed_unique_content_bytes": self.space_delta(
                initial_space,
                final_space,
                ("logical_retained", "observed_unique_content_bytes"),
            ),
            "logical_live_file_bytes": self.space_delta(
                initial_space, final_space, ("logical_live", "file_bytes")
            ),
            "noncurrent_versions": "unknown",
            "deletion_authorized": False,
            "reclaimable_bytes": None,
        }
        remount_unmount = self.mount.stop(self.redact)
        final_restore = self.restore_remote(final_bytes, "final")
        doctor = self.doctor()
        cleanup = self.cleanup_remote()
        self.report.update(
            {
                "status": "pass",
                "finished_at": utc_now(),
                "elapsed_seconds": round(time.monotonic() - self.started, 3),
                "workload": {
                    "initial_bytes": INITIAL_BYTES,
                    "append_bytes": len(APPEND_BYTES),
                    "zero_extend_bytes": ZERO_EXTEND_BYTES,
                    "shrink_bytes": SHRINK_BYTES,
                    "shrink_extend_bytes": EXTEND_BYTES,
                    "remount_append_bytes": len(REMOUNT_APPEND_BYTES),
                    "final_length": len(final_bytes),
                    "final_sha256": bytes_sha256(final_bytes),
                    "seed": self.seed,
                    "operations": [
                        "initial-write",
                        "append",
                        "zero-extend",
                        "shrink-extend",
                        "clean-unmount",
                        "remount-byte-verification",
                        "remount-append",
                        "space-report-while-mounted",
                        "clean-unmount",
                    ],
                },
                "remount": {
                    "first_clean_unmount": first["first_clean_unmount"],
                    "append_restore": first["append_restore"],
                    "restored_exact_bytes": True,
                    "space_report_while_mounted": True,
                    "final_clean_unmount": remount_unmount,
                    "final_restore": final_restore,
                },
                "space": {
                    "initial": initial_space,
                    "final": final_space,
                    "comparison": space_comparison,
                },
                "doctor": doctor,
                "cleanup": cleanup,
            }
        )
        self.publish_report()
        return self.report

    def close(self, success: bool) -> None:
        self.mount.force_unmount()
        if not success:
            print(
                "stage-3 real-S3 failure evidence retained: "
                f"work={self.work} remote_prefix=s3://{self.bucket}/{self.root}/",
                file=sys.stderr,
                flush=True,
            )
            return
        try:
            if self.aws_cli and shutil.which(self.aws_cli):
                self.cleanup_remote()
        except Exception:
            pass
        shutil.rmtree(self.work, ignore_errors=True)


def main() -> int:
    credentials = credentials_from_stdin()
    trial = Trial(credentials)
    success = False
    try:
        report = trial.execute()
        print(json.dumps(report, sort_keys=True), flush=True)
        success = True
        return 0
    except Exception as error:
        trial.report.update(
            {
                "status": "fail",
                "finished_at": utc_now(),
                "elapsed_seconds": round(time.monotonic() - trial.started, 3),
                "error": trial.redact(str(error)),
                "retained": {
                    "work": str(trial.work),
                    "remote_prefix": f"s3://{trial.bucket}/{trial.root}/",
                },
            }
        )
        try:
            trial.publish_report()
        except Exception:
            pass
        print(trial.redact(str(error)), file=sys.stderr, flush=True)
        return 1
    finally:
        trial.close(success)


if __name__ == "__main__":
    sys.exit(main())
