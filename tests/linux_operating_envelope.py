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

"""Run a bounded Linux FUSE operating-envelope sample against real S3.

The harness enters through one installed yy binary and an ordinary FUSE mount.
It records process RSS/I/O, local state allocation, remote object-version
growth, operation latencies and independent final bytes. It does not enable
paid storage metrics or claim a production capacity limit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "yinyang.linux-operating-envelope.v1"
REQUEST_SCHEMA = "yinyang.linux-request-accounting.v1"
STORAGE_NAMES = (
    "YINYANG_STORAGE_PROFILE",
    "YINYANG_S3_BUCKET",
    "YINYANG_S3_REGION",
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
    "YINYANG_S3_SESSION_TOKEN",
    "YINYANG_S3_ENDPOINT",
)
SECRET_NAMES = (
    "YINYANG_S3_ACCESS_KEY_ID",
    "YINYANG_S3_SECRET_ACCESS_KEY",
    "YINYANG_S3_SESSION_TOKEN",
)
PAGE_BYTES = 4096
POLL_SECONDS = 0.05
MAX_FIRST_ERRORS = 16
FSYNC_RETRY_SECONDS = 30.0
FSYNC_RETRY_INTERVAL = 0.5
FSYNC_RETRY_MAX_INTERVAL = 5.0


class EnvelopeError(RuntimeError):
    """A qualification precondition or observed workload invariant failed."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def binary_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bytes_sha256(bytes_: bytes | bytearray) -> str:
    return hashlib.sha256(bytes_).hexdigest()


def tree_usage(root: Path) -> dict[str, int]:
    files = 0
    logical = 0
    allocated = 0
    if not root.exists():
        return {"files": 0, "logical_bytes": 0, "allocated_bytes": 0}
    for current, directories, names in os.walk(root):
        for name in directories:
            path = Path(current, name)
            try:
                allocated += path.lstat().st_blocks * 512
            except FileNotFoundError:
                pass
        for name in names:
            path = Path(current, name)
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                continue
            files += 1
            logical += metadata.st_size
            allocated += metadata.st_blocks * 512
    return {
        "files": files,
        "logical_bytes": logical,
        "allocated_bytes": allocated,
    }


def read_proc_status(pid: int | None) -> dict[str, int | None]:
    if pid is None:
        return {"rss_bytes": None, "hwm_bytes": None, "threads": None}
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    except FileNotFoundError:
        return {"rss_bytes": None, "hwm_bytes": None, "threads": None}
    values: dict[str, int | None] = {
        "rss_bytes": None,
        "hwm_bytes": None,
        "threads": None,
    }
    for line in lines:
        name, _, raw = line.partition(":")
        if name == "VmRSS":
            values["rss_bytes"] = int(raw.split()[0]) * 1024
        elif name == "VmHWM":
            values["hwm_bytes"] = int(raw.split()[0]) * 1024
        elif name == "Threads":
            values["threads"] = int(raw.strip())
    return values


def read_proc_io(pid: int | None) -> dict[str, int | None]:
    names = ("rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes")
    values: dict[str, int | None] = {name: None for name in names}
    if pid is None:
        return values
    try:
        lines = Path(f"/proc/{pid}/io").read_text().splitlines()
    except FileNotFoundError:
        return values
    for line in lines:
        name, _, raw = line.partition(":")
        if name in values:
            values[name] = int(raw.strip())
    return values


def io_since(
    before: dict[str, int | None], after: dict[str, int | None]
) -> dict[str, int | None]:
    values: dict[str, int | None] = {}
    for name, earlier in before.items():
        later = after.get(name)
        if earlier is None or later is None:
            values[name] = None
        else:
            values[name] = max(0, later - earlier)
    return values


def io_add(total: dict[str, int], sample: dict[str, int | None]) -> None:
    for name, value in sample.items():
        if value is not None:
            total[name] = total.get(name, 0) + value


def fd_count(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        return sum(1 for _ in Path(f"/proc/{pid}/fd").iterdir())
    except (FileNotFoundError, PermissionError):
        return None


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def latency_summary(values: list[float], errors: int) -> dict[str, Any]:
    return {
        "operations": len(values) + errors,
        "successes": len(values),
        "errors": errors,
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "max_ms": max(values) if values else None,
        "mean_ms": statistics.fmean(values) if values else None,
    }


def parse_os_release() -> dict[str, str]:
    path = Path("/etc/os-release")
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        name, separator, raw = line.partition("=")
        if not separator:
            continue
        values[name] = raw.strip().strip('"')
    return values


def host_identity() -> dict[str, Any]:
    os_release = parse_os_release()
    return {
        "hostname": platform.node(),
        "machine": platform.machine(),
        "kernel": platform.release(),
        "os_id": os_release.get("ID"),
        "os_version_id": os_release.get("VERSION_ID"),
        "os_pretty_name": os_release.get("PRETTY_NAME"),
        "cpu_count": os.cpu_count(),
    }


def mount_type(mountpoint: Path) -> str | None:
    for line in Path("/proc/self/mounts").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 3 and Path(fields[1]) == mountpoint:
            return fields[2]
    return None


def write_private(path: Path, bytes_: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(bytes_)
        while view:
            written = os.write(descriptor, view)
            if written == 0:
                raise EnvelopeError(f"short write to {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def wait_for(predicate: Any, timeout: float, context: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(POLL_SECONDS)
    raise EnvelopeError(f"timed out after {timeout:.1f}s waiting for {context}")


@dataclass
class Completed:
    stdout: str
    stderr: str
    returncode: int


def file_status_snapshot(stdout: str) -> dict[str, Any]:
    declared_handles = None
    handles = []
    errors = []
    failed = 0
    for line in stdout.splitlines():
        if line.startswith("local handles: "):
            if declared_handles is not None:
                raise EnvelopeError("status reported local handles more than once")
            declared_handles = int(line.removeprefix("local handles: "))
        elif line.startswith("handle: "):
            fields = line.split()
            if (
                len(fields) != 14
                or fields[8] != "pending:"
                or fields[10] != "frozen:"
                or fields[12] != "conflict:"
            ):
                raise EnvelopeError(f"unrecognized handle status line: {line}")
            pending = fields[9]
            frozen = fields[11]
            conflict = fields[13]
            if pending not in {"true", "false"}:
                raise EnvelopeError(f"invalid pending state: {pending}")
            if frozen not in {"true", "false"}:
                raise EnvelopeError(f"invalid frozen state: {frozen}")
            if conflict not in {"true", "false"}:
                raise EnvelopeError(f"invalid conflict state: {conflict}")
            handles.append(
                {
                    "pending": pending == "true",
                    "frozen": frozen == "true",
                    "conflict": conflict == "true",
                }
            )
        elif line.startswith("handle error: "):
            failed += 1
        elif line.startswith("error "):
            fields = line.split(maxsplit=6)
            if len(fields) < 6 or fields[2] != "handle" or fields[4] != "persisted":
                raise EnvelopeError(f"unrecognized ledger status line: {line}")
            persisted = fields[5]
            if persisted not in {"true:", "false:"}:
                raise EnvelopeError(f"invalid persisted state: {persisted}")
            errors.append(
                {
                    "sequence": int(fields[1]),
                    "handle": fields[3],
                    "persisted": persisted == "true:",
                }
            )
    if declared_handles is None or declared_handles != len(handles):
        raise EnvelopeError(
            "status local handle count does not match printed handle records"
        )
    if failed > len(handles):
        raise EnvelopeError("status printed more handle errors than handles")
    sequences = [error["sequence"] for error in errors]
    return {
        "local_handles": declared_handles,
        "pending": sum(handle["pending"] for handle in handles),
        "frozen": sum(handle["frozen"] for handle in handles),
        "conflicts": sum(handle["conflict"] for handle in handles),
        "failed": failed,
        "ledger_errors": len(errors),
        "sequences": sequences,
        "errors": errors,
    }


class MountProcess:
    def __init__(
        self,
        yy: Path,
        env: dict[str, str],
        config: Path,
        mountpoint: Path,
        log_root: Path,
    ) -> None:
        self.yy = yy
        self.env = env
        self.config = config
        self.mountpoint = mountpoint
        self.log_root = log_root
        self.process: subprocess.Popen[bytes] | None = None
        self.stdout: threading.Thread | None = None
        self.stderr: threading.Thread | None = None

    def start(self, redact: Any) -> None:
        if mount_type(self.mountpoint) is not None:
            raise EnvelopeError(f"{self.mountpoint} is already mounted")
        stdout = (self.log_root / "mount.stdout").open("ab")
        stderr = (self.log_root / "mount.stderr").open("ab")
        self.process = subprocess.Popen(
            [str(self.yy), "--volume", str(self.config), "mount", str(self.mountpoint)],
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
        )

        def close_stream(stream: Any) -> None:
            stream.close()

        self.stdout = threading.Thread(
            target=close_stream, args=(stdout,), daemon=True
        )
        self.stderr = threading.Thread(
            target=close_stream, args=(stderr,), daemon=True
        )
        self.stdout.start()
        self.stderr.start()
        try:
            wait_for(
                lambda: mount_type(self.mountpoint) == "fuse.yinyang"
                or self.process.poll() is not None,
                60,
                "fuse.yinyang mount readiness",
            )
            if self.process.poll() is not None:
                raise EnvelopeError(
                    f"yy mount exited before readiness:\n{self.diagnostics(redact)}"
                )
        except Exception:
            self.force_unmount()
            raise

    def diagnostics(self, redact: Any) -> str:
        stderr = (self.log_root / "mount.stderr").read_text(errors="replace")
        stdout = (self.log_root / "mount.stdout").read_text(errors="replace")
        return redact(f"stdout:\n{stdout}\nstderr:\n{stderr}")

    def pid(self) -> int | None:
        if self.process is None or self.process.poll() is not None:
            return None
        return self.process.pid

    def stop(self, redact: Any) -> dict[str, Any]:
        if self.process is None:
            return {"stopped": True, "returncode": None}
        if mount_type(self.mountpoint) is not None:
            unmount = subprocess.run(
                ["fusermount3", "-u", str(self.mountpoint)],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if unmount.returncode != 0:
                raise EnvelopeError(
                    redact(
                        "fusermount3 failed with "
                        f"{unmount.returncode}:\n{unmount.stdout}\n{unmount.stderr}"
                    )
                )
        try:
            returncode = self.process.wait(timeout=30)
        except subprocess.TimeoutExpired as error:
            self.process.kill()
            returncode = self.process.wait(timeout=10)
            raise EnvelopeError(
                f"yy mount did not exit after unmount; final status {returncode}\n"
                f"{self.diagnostics(redact)}"
            ) from error
        if self.stdout is not None:
            self.stdout.join(timeout=5)
        if self.stderr is not None:
            self.stderr.join(timeout=5)
        self.process = None
        if returncode != 0:
            raise EnvelopeError(
                f"yy mount exited with {returncode} after a clean unmount:\n"
                f"{self.diagnostics(redact)}"
            )
        return {"stopped": True, "returncode": returncode}

    def force_unmount(self) -> None:
        if mount_type(self.mountpoint) is not None:
            subprocess.run(
                ["fusermount3", "-u", "-z", str(self.mountpoint)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
            )
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        self.process = None


class Workload:
    def __init__(
        self,
        mountpoint: Path,
        expected: list[bytearray],
        writers: int,
        readers: int,
        write_interval: float,
        read_interval: float,
        seed: int,
        fsync_retry_seconds: float = FSYNC_RETRY_SECONDS,
        fsync_retry_interval: float = FSYNC_RETRY_INTERVAL,
    ) -> None:
        self.mountpoint = mountpoint
        self.expected = expected
        self.writers = writers
        self.readers = readers
        self.write_interval = write_interval
        self.read_interval = read_interval
        self.seed = seed
        self.fsync_retry_seconds = fsync_retry_seconds
        self.fsync_retry_interval = fsync_retry_interval
        self.stop = threading.Event()
        self.threads: list[threading.Thread] = []
        self.lock = threading.Lock()
        self.write_latencies: list[float] = []
        self.read_latencies: list[float] = []
        self.pwrite_errors = 0
        self.fsync_errors = 0
        self.uncertain_files: set[int] = set()
        self.read_errors = 0
        self.fatal: list[str] = []
        self.first_errors: list[dict[str, Any]] = []
        self.started_at: float | None = None
        self.fsync_retry_attempts = 0
        self.fsync_retry_successes = 0

    def start(self) -> None:
        self.started_at = time.monotonic()
        for worker in range(self.writers):
            thread = threading.Thread(
                target=self.writer,
                args=(worker,),
                name=f"soak-writer-{worker}",
                daemon=True,
            )
            self.threads.append(thread)
            thread.start()
        for worker in range(self.readers):
            thread = threading.Thread(
                target=self.reader,
                args=(worker,),
                name=f"soak-reader-{worker}",
                daemon=True,
            )
            self.threads.append(thread)
            thread.start()

    def path(self, index: int) -> Path:
        return self.mountpoint / f"file-{index:04d}.bin"

    def writer(self, worker: int) -> None:
        random_source = random.Random(self.seed + worker)
        descriptors: dict[int, int] = {}
        failures = 0
        try:
            for index in range(worker, len(self.expected), self.writers):
                descriptors[index] = os.open(self.path(index), os.O_RDWR)
            while not self.stop.is_set():
                started = time.monotonic()
                index = random_source.choice(list(descriptors))
                maximum_page = (len(self.expected[index]) // PAGE_BYTES) - 1
                offset = random_source.randint(0, maximum_page) * PAGE_BYTES
                page = random_source.randbytes(PAGE_BYTES)
                try:
                    view = memoryview(page)
                    written = 0
                    while view:
                        count = os.pwrite(descriptors[index], view, offset + written)
                        if count <= 0:
                            raise OSError("short pwrite through FUSE")
                        segment = page[written : written + count]
                        self.expected[index][
                            offset + written : offset + written + count
                        ] = segment
                        written += count
                        view = view[count:]
                except OSError as error:
                    self.record_first_error("pwrite", index, error)
                    with self.lock:
                        self.pwrite_errors += 1
                        self.uncertain_files.add(index)
                    failures += 1
                    if failures >= 3:
                        self.fatal.append(f"three consecutive writes failed: {error}")
                        self.stop.set()
                else:
                    if not self.fsync_with_retry(descriptors[index], index):
                        self.fatal.append(
                            f"fsync retry deadline expired for file-{index:04d}.bin"
                        )
                        self.stop.set()
                    else:
                        with self.lock:
                            self.write_latencies.append(
                                (time.monotonic() - started) * 1000
                            )
                            self.uncertain_files.discard(index)
                        failures = 0
                elapsed = time.monotonic() - started
                self.stop.wait(max(0.0, self.write_interval - elapsed))
        except OSError as error:
            self.record_first_error("open", None, error)
            self.fatal.append(f"writer {worker} failed outside an operation: {error}")
            self.stop.set()
        finally:
            for descriptor in descriptors.values():
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def reader(self, worker: int) -> None:
        random_source = random.Random(self.seed + 10_000 + worker)
        failures = 0
        while not self.stop.is_set():
            started = time.monotonic()
            index = random_source.randrange(len(self.expected))
            maximum_page = (len(self.expected[index]) // PAGE_BYTES) - 1
            offset = random_source.randint(0, maximum_page) * PAGE_BYTES
            try:
                descriptor = os.open(self.path(index), os.O_RDONLY)
                try:
                    bytes_ = os.pread(descriptor, PAGE_BYTES, offset)
                finally:
                    os.close(descriptor)
                if len(bytes_) != PAGE_BYTES:
                    raise OSError(
                        f"short FUSE read at {self.path(index)}:{offset}: "
                        f"{len(bytes_)} bytes"
                    )
                with self.lock:
                    self.read_latencies.append((time.monotonic() - started) * 1000)
                failures = 0
            except OSError as error:
                self.record_first_error("read", index, error)
                with self.lock:
                    self.read_errors += 1
                failures += 1
                if failures >= 3:
                    self.fatal.append(f"three consecutive reads failed: {error}")
                    self.stop.set()
            elapsed = time.monotonic() - started
            self.stop.wait(max(0.0, self.read_interval - elapsed))

    def fsync_with_retry(self, descriptor: int, index: int) -> bool:
        try:
            os.fsync(descriptor)
            return True
        except OSError as error:
            self.record_first_error("fsync", index, error)
            with self.lock:
                self.fsync_errors += 1
                self.uncertain_files.add(index)

        deadline = time.monotonic() + self.fsync_retry_seconds
        interval = self.fsync_retry_interval
        # Drain an already-dispatched fsync even if the phase ends normally.
        # A normal phase stop must not turn a recoverable in-flight publication
        # into an artificial deadline failure.
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(interval, remaining))
            if time.monotonic() >= deadline:
                break
            with self.lock:
                self.fsync_retry_attempts += 1
            try:
                os.fsync(descriptor)
            except OSError as error:
                self.record_first_error("fsync-retry", index, error)
                with self.lock:
                    self.fsync_errors += 1
                    self.uncertain_files.add(index)
                interval = min(interval * 2, FSYNC_RETRY_MAX_INTERVAL)
                continue
            with self.lock:
                self.fsync_retry_successes += 1
                self.uncertain_files.discard(index)
            return True
        return False

    def record_first_error(
        self, operation: str, index: int | None, error: OSError
    ) -> None:
        with self.lock:
            if len(self.first_errors) >= MAX_FIRST_ERRORS:
                return
            self.first_errors.append(
                {
                    "operation": operation,
                    "file": None if index is None else f"file-{index:04d}.bin",
                    "errno": error.errno,
                    "message": str(error),
                    "elapsed_seconds": (
                        None
                        if self.started_at is None
                        else round(time.monotonic() - self.started_at, 3)
                    ),
                }
            )

    def finish(self) -> dict[str, Any]:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=self.write_interval + self.read_interval + 30)
        if any(thread.is_alive() for thread in self.threads):
            raise EnvelopeError("workload thread did not stop within its bounded timeout")
        write_errors = self.pwrite_errors + self.fsync_errors
        writers = {
            **latency_summary(self.write_latencies, write_errors),
            "interval_seconds": self.write_interval,
            "threads": self.writers,
            "bytes_per_write": PAGE_BYTES,
            "fsync_each_write": True,
            "pwrite_errors": self.pwrite_errors,
            "fsync_errors": self.fsync_errors,
            "fsync_retry_attempts": self.fsync_retry_attempts,
            "fsync_retry_successes": self.fsync_retry_successes,
            "fsync_retry_seconds": self.fsync_retry_seconds,
            "fsync_retry_interval": self.fsync_retry_interval,
            "uncertain_files": sorted(self.uncertain_files),
        }
        return {
            "fatal": list(self.fatal),
            "first_errors": self.first_errors,
            "writers": writers,
            "readers": {
                **latency_summary(self.read_latencies, self.read_errors),
                "interval_seconds": self.read_interval,
                "threads": self.readers,
                "bytes_per_read": PAGE_BYTES,
            },
        }


class Harness:
    def __init__(self, args: argparse.Namespace, report: dict[str, Any]) -> None:
        self.args = args
        self.report = report
        self.yy = Path(args.yy).resolve()
        self.output = Path(args.output).resolve()
        self.work_root = Path(args.work_root).resolve()
        self.work_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.work = Path(
            tempfile.mkdtemp(prefix="linux-envelope-", dir=self.work_root)
        )
        os.chmod(self.work, 0o700)
        self.config_dir = self.work / "config"
        self.state = self.work / "state"
        self.mountpoint = self.work / "mount"
        self.log_root = self.work / "logs"
        self.verify_root = self.work / "verify"
        for directory in (
            self.config_dir,
            self.state,
            self.mountpoint,
            self.log_root,
            self.verify_root,
        ):
            directory.mkdir(mode=0o700)
        self.source_environment = {
            name: os.environ[name] for name in STORAGE_NAMES if name in os.environ
        }
        self.secrets = [
            self.source_environment[name]
            for name in SECRET_NAMES
            if self.source_environment.get(name)
        ]
        self.root = f"linux-operating-envelope/{uuid.uuid4().hex}"
        self.env = self.storage_environment()
        self.aws_cli = os.environ.get("AWS_CLI_BIN", "aws")
        self.aws_env = self.aws_environment()
        self.config = self.config_dir / "envelope.json"
        self.mount = MountProcess(
            self.yy, self.env, self.config, self.mountpoint, self.log_root
        )
        self.samples: list[dict[str, Any]] = []
        self.history_samples: list[dict[str, Any]] = []
        self.io_totals: dict[str, int] = {}
        self.last_io_pid: int | None = None
        self.last_io: dict[str, int | None] | None = None
        self.report["samples"] = self.samples
        self.report["history_samples"] = self.history_samples
        self.report["workdir"] = str(self.work)
        self.report["backend"]["storage_root"] = self.root

    def redact(self, text: str) -> str:
        output = text
        for secret in self.secrets:
            output = output.replace(secret, "<redacted>")
        return output

    def storage_environment(self) -> dict[str, str]:
        profile = self.source_environment.get("YINYANG_STORAGE_PROFILE")
        if profile != "amazon-s3":
            raise EnvelopeError(
                "YINYANG_STORAGE_PROFILE must be amazon-s3 for non-loopback evidence"
            )
        if self.source_environment.get("YINYANG_S3_ENDPOINT"):
            raise EnvelopeError(
                "YINYANG_S3_ENDPOINT must be unset for the Amazon S3 qualification"
            )
        for name in (
            "YINYANG_S3_BUCKET",
            "YINYANG_S3_REGION",
            "YINYANG_S3_ACCESS_KEY_ID",
            "YINYANG_S3_SECRET_ACCESS_KEY",
        ):
            if not self.source_environment.get(name):
                raise EnvelopeError(f"{name} is required")
        env = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith("YINYANG_")
        }
        for name, value in self.source_environment.items():
            env[name] = value
        env["YINYANG_STORAGE_PROFILE"] = "amazon-s3"
        env["YINYANG_S3_ROOT"] = self.root
        env["AWS_EC2_METADATA_DISABLED"] = "true"
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
        env["AWS_ACCESS_KEY_ID"] = self.source_environment[
            "YINYANG_S3_ACCESS_KEY_ID"
        ]
        env["AWS_SECRET_ACCESS_KEY"] = self.source_environment[
            "YINYANG_S3_SECRET_ACCESS_KEY"
        ]
        if token := self.source_environment.get("YINYANG_S3_SESSION_TOKEN"):
            env["AWS_SESSION_TOKEN"] = token
        else:
            env.pop("AWS_SESSION_TOKEN", None)
        env["AWS_REGION"] = self.source_environment["YINYANG_S3_REGION"]
        env["AWS_DEFAULT_REGION"] = self.source_environment["YINYANG_S3_REGION"]
        env["AWS_CONFIG_FILE"] = os.devnull
        env["AWS_SHARED_CREDENTIALS_FILE"] = os.devnull
        env["AWS_EC2_METADATA_DISABLED"] = "true"
        env["AWS_PAGER"] = ""
        env["AWS_CLI_BIN"] = self.aws_cli
        return env

    def run(
        self,
        command: list[str],
        env: dict[str, str],
        timeout: float = 180,
        check: bool = True,
    ) -> Completed:
        completed = subprocess.run(
            command,
            env=env,
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
        return Completed(stdout=stdout, stderr=stderr, returncode=completed.returncode)

    def yy_run(
        self, arguments: list[str], timeout: float = 180, check: bool = True
    ) -> Completed:
        return self.run(
            [str(self.yy), *arguments], self.env, timeout=timeout, check=check
        )

    def aws(self, arguments: list[str], timeout: float = 120) -> dict[str, Any]:
        completed = self.run(
            [
                self.aws_cli,
                "s3api",
                *arguments,
                "--region",
                self.source_environment["YINYANG_S3_REGION"],
                "--no-cli-pager",
                "--output",
                "json",
            ],
            self.aws_env,
            timeout=timeout,
        )
        if not completed.stdout.strip():
            return {}
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise EnvelopeError(f"AWS CLI returned invalid JSON: {error}") from error

    def preflight(self) -> None:
        if platform.system() != "Linux" or platform.machine() != "x86_64":
            raise EnvelopeError("operating-envelope qualification requires Linux x86_64")
        for command in ("fusermount3", "sqlite3"):
            if shutil.which(command) is None:
                raise EnvelopeError(f"required command is unavailable: {command}")
        if shutil.which(self.aws_cli) is None:
            raise EnvelopeError(f"AWS CLI is not executable: {self.aws_cli}")
        if not Path("/dev/fuse").exists():
            raise EnvelopeError("/dev/fuse is unavailable")
        if not self.yy.is_file():
            raise EnvelopeError(f"yy binary is unavailable: {self.yy}")
        self.request_accounting = self.load_request_accounting()
        versioning = self.aws(
            [
                "get-bucket-versioning",
                "--bucket",
                self.source_environment["YINYANG_S3_BUCKET"],
            ]
        )
        if versioning.get("Status") != "Enabled":
            raise EnvelopeError(
                "the dedicated qualification bucket must have versioning enabled"
            )
        # Establish pagination and permission before spending the soak interval.
        self.remote_history(0.0)

    def write_config(self) -> None:
        document = {
            "name": "operating-envelope",
            "model": "managed",
            "access": "mount",
            "frontend": "fuse",
            "publication": "object",
            "storage_profile": "amazon-s3",
            "staging": str(self.state),
            "read_only": False,
            "require": [],
        }
        write_private(self.config, json.dumps(document, indent=2).encode() + b"\n")

    def prepare_authority(self) -> None:
        self.write_config()
        self.yy_run(["create"], timeout=300)
        self.yy_run(["--volume", str(self.config), "capabilities"], timeout=300)

    def create_dataset(self) -> list[bytearray]:
        dataset = [
            bytearray(
                random.Random(self.args.seed + index).randbytes(self.args.file_bytes)
            )
            for index in range(self.args.files)
        ]
        for index, bytes_ in enumerate(dataset):
            path = self.mountpoint / f"file-{index:04d}.bin"
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            try:
                view = memoryview(bytes_)
                while view:
                    count = os.write(descriptor, view)
                    if count <= 0:
                        raise EnvelopeError(f"short initial write to {path}")
                    view = view[count:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self.report["dataset"] = {
            "files": self.args.files,
            "file_bytes": self.args.file_bytes,
            "logical_bytes": self.args.files * self.args.file_bytes,
            "page_bytes": PAGE_BYTES,
            "seed": self.args.seed,
        }
        return dataset

    def sample(self, elapsed: float, phase: str) -> dict[str, Any]:
        pid = self.mount.pid()
        io = read_proc_io(pid)
        if pid is not None:
            if self.last_io_pid == pid and self.last_io is not None:
                io_add(self.io_totals, io_since(self.last_io, io))
            else:
                io_add(self.io_totals, io)
            self.last_io_pid = pid
            self.last_io = io
        else:
            self.last_io_pid = None
            self.last_io = None
        statistics_ = os.statvfs(self.state)
        sample = {
            "timestamp": utc_now(),
            "elapsed_seconds": round(elapsed, 3),
            "phase": phase,
            "pid": pid,
            "process": read_proc_status(pid),
            "open_fds": fd_count(pid),
            "state": tree_usage(self.state),
            "filesystem_available_bytes": statistics_.f_bavail
            * statistics_.f_frsize,
            "process_io_total": io,
        }
        self.samples.append(sample)
        return sample

    def remote_history(self, elapsed: float) -> dict[str, Any]:
        bucket = self.source_environment["YINYANG_S3_BUCKET"]
        versions = 0
        delete_markers = 0
        stored_bytes = 0
        pages = 0
        key_marker: str | None = None
        version_marker: str | None = None
        while True:
            arguments = [
                "list-object-versions",
                "--bucket",
                bucket,
                "--prefix",
                f"{self.root}/",
                "--no-paginate",
            ]
            if key_marker is not None:
                arguments.extend(["--key-marker", key_marker])
            if version_marker is not None:
                arguments.extend(["--version-id-marker", version_marker])
            response = self.aws(arguments, timeout=300)
            pages += 1
            page_versions = response.get("Versions", [])
            versions += len(page_versions)
            delete_markers += len(response.get("DeleteMarkers", []))
            stored_bytes += sum(int(item.get("Size", 0)) for item in page_versions)
            if not response.get("IsTruncated", False):
                break
            key_marker = response.get("NextKeyMarker")
            version_marker = response.get("NextVersionIdMarker")
            if not key_marker or not version_marker:
                raise EnvelopeError(
                    "ListObjectVersions omitted continuation markers for a truncated page"
                )
        sample = {
            "timestamp": utc_now(),
            "elapsed_seconds": round(elapsed, 3),
            "object_versions": versions,
            "delete_markers": delete_markers,
            "stored_bytes": stored_bytes,
            "pages": pages,
            "method": "paginated ListObjectVersions over the isolated prefix",
        }
        self.history_samples.append(sample)
        return sample

    def run_phase(
        self, name: str, duration: float, expected: list[bytearray]
    ) -> dict[str, Any]:
        workload = Workload(
            mountpoint=self.mountpoint,
            expected=expected,
            writers=self.args.writers,
            readers=self.args.readers,
            write_interval=self.args.write_interval,
            read_interval=self.args.read_interval,
            seed=self.args.seed,
            fsync_retry_seconds=self.args.fsync_retry_seconds,
            fsync_retry_interval=self.args.fsync_retry_interval,
        )
        phase_started = time.monotonic()
        phase_deadline = phase_started + duration
        workload.start()
        next_sample = phase_started
        next_history = phase_started + self.args.history_interval
        try:
            while time.monotonic() < phase_deadline:
                now = time.monotonic()
                if now >= next_sample:
                    self.sample(now - self.report_started, name)
                    next_sample = now + self.args.sample_interval
                if now >= next_history:
                    self.remote_history(now - self.report_started)
                    next_history = now + self.args.history_interval
                if workload.fatal:
                    raise EnvelopeError("; ".join(workload.fatal))
                time.sleep(
                    min(
                        0.5,
                        max(0.0, phase_deadline - time.monotonic()),
                        max(0.0, next_sample - time.monotonic()),
                    )
                )
        finally:
            summary = workload.finish()
            summary["pwrite_acknowledged_expected"] = {
                "basis": "pwrite-acknowledged expected bytes; fsync uncertainty is separate",
                "files": [
                    {
                        "file": f"file-{index:04d}.bin",
                        "length": len(bytes_),
                        "sha256": bytes_sha256(bytes_),
                    }
                    for index, bytes_ in enumerate(expected)
                ],
            }
            summary["planned_seconds"] = duration
            summary["actual_seconds"] = round(time.monotonic() - phase_started, 3)
            summary["phase_started_report_elapsed_seconds"] = round(
                phase_started - self.report_started, 3
            )
            self.report.setdefault("phases", {})[name] = summary
            self.sample(time.monotonic() - self.report_started, f"{name}-final")
        if summary["fatal"]:
            raise EnvelopeError("; ".join(summary["fatal"]))
        return summary

    def verify_bytes(self, label: str, expected: list[bytearray]) -> dict[str, Any]:
        destination = self.verify_root / label
        self.yy_run(["restore", str(destination)], timeout=900)
        expected_names = {f"file-{index:04d}.bin" for index in range(len(expected))}
        actual_names = {entry.name for entry in destination.iterdir()}
        if actual_names != expected_names:
            raise EnvelopeError(
                f"{label} restore path set differs: "
                f"missing={sorted(expected_names - actual_names)} "
                f"extra={sorted(actual_names - expected_names)}"
            )
        total = 0
        sha256 = hashlib.sha256()
        for index, expected_bytes in enumerate(expected):
            path = destination / f"file-{index:04d}.bin"
            actual = path.read_bytes()
            if actual != expected_bytes:
                raise EnvelopeError(
                    f"{label} restore byte mismatch for {path.name}: "
                    f"expected {len(expected_bytes)} bytes sha256={bytes_sha256(expected_bytes)}, "
                    f"got {len(actual)} bytes sha256={bytes_sha256(actual)}"
                )
            total += len(actual)
            sha256.update(path.name.encode())
            sha256.update(actual)
        return {"files": len(expected), "logical_bytes": total, "sha256": sha256.hexdigest()}

    def verify_mount_bytes(self, expected: list[bytearray]) -> None:
        for index, expected_bytes in enumerate(expected):
            actual = (self.mountpoint / f"file-{index:04d}.bin").read_bytes()
            if actual != expected_bytes:
                raise EnvelopeError(
                    f"mounted bytes mismatch for file-{index:04d}.bin: "
                    f"expected {len(expected_bytes)} bytes sha256={bytes_sha256(expected_bytes)}, "
                    f"got {len(actual)} bytes sha256={bytes_sha256(actual)}"
                )

    def sqlite_integrity(self) -> list[dict[str, Any]]:
        results = []
        for database in sorted(self.state.rglob("*.db")):
            connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
            try:
                result = connection.execute("PRAGMA integrity_check").fetchone()
            finally:
                connection.close()
            if result is None or result[0] != "ok":
                raise EnvelopeError(
                    f"SQLite integrity_check failed for {database}: {result}"
                )
            results.append(
                {
                    "path": database.relative_to(self.state).as_posix(),
                    "integrity_check": result[0],
                }
            )
        return results

    def doctor(self) -> dict[str, Any]:
        output = self.yy_run(
            ["--volume", str(self.config), "doctor", "--json"], timeout=180
        )
        return json.loads(output.stdout)

    def load_request_accounting(self) -> dict[str, Any]:
        if not self.args.request_accounting:
            return {
                "status": "unavailable",
                "reason": (
                    "Amazon S3 TLS traffic is outside the plain-HTTP counting proxy; "
                    "no process-scoped client counter was supplied"
                ),
            }
        path = Path(self.args.request_accounting).resolve()
        document = json.loads(path.read_text())
        if document.get("schema") != REQUEST_SCHEMA:
            raise EnvelopeError(
                f"request accounting must use schema {REQUEST_SCHEMA}"
            )
        if document.get("source_sha") != self.args.source_sha:
            raise EnvelopeError("request accounting source_sha does not match --source-sha")
        if document.get("binary_sha256") != self.report["binary"]["sha256"]:
            raise EnvelopeError(
                "request accounting binary_sha256 does not match the measured yy"
            )
        return {"status": "provided", **document}

    def finalize_statistics(self) -> None:
        rss = [
            sample["process"]["rss_bytes"]
            for sample in self.samples
            if sample["process"]["rss_bytes"] is not None
        ]
        hwm = [
            sample["process"]["hwm_bytes"]
            for sample in self.samples
            if sample["process"]["hwm_bytes"] is not None
        ]
        self.report["summary"] = {
            "samples": len(self.samples),
            "max_rss_bytes": max(rss) if rss else None,
            "max_hwm_bytes": max(hwm) if hwm else None,
            "max_state_allocated_bytes": max(
                (sample["state"]["allocated_bytes"] for sample in self.samples),
                default=None,
            ),
            "max_fds": max(
                (
                    sample["open_fds"]
                    for sample in self.samples
                    if sample["open_fds"] is not None
                ),
                default=None,
            ),
            "process_io_cumulative": {
                name: value for name, value in sorted(self.io_totals.items())
            },
        }

    def execute(self) -> None:
        self.preflight()
        self.report["started_at"] = utc_now()
        self.report_started = time.monotonic()
        self.report["phase"] = "mount-start"
        self.prepare_authority()
        self.mount.start(self.redact)
        expected = self.create_dataset()
        self.remote_history(time.monotonic() - self.report_started)
        first_duration = self.args.duration_seconds / 2
        second_duration = self.args.duration_seconds - first_duration
        self.report["phase"] = "steady-state-one"
        first = self.run_phase("steady-state-one", first_duration, expected)
        self.verify_mount_bytes(expected)
        self.report["phase"] = "restart"
        stopped_one = self.mount.stop(self.redact)
        first_verify = self.verify_bytes("first-phase", expected)
        self.remote_history(time.monotonic() - self.report_started)
        self.mount.start(self.redact)
        self.verify_mount_bytes(expected)
        self.report["restart"] = {
            "clean_unmount": stopped_one,
            "restored_bytes": first_verify,
            "remount": "fuse.yinyang",
        }
        self.report["phase"] = "steady-state-two"
        second = self.run_phase("steady-state-two", second_duration, expected)
        self.report["phases"] = {
            "steady-state-one": first,
            "steady-state-two": second,
        }
        self.finalize_statistics()
        self.report["request_accounting"] = self.request_accounting
        uncertain_files = sorted(
            set(first["writers"]["uncertain_files"])
            | set(second["writers"]["uncertain_files"])
        )
        self.report["durability"] = {
            "status": "unknown" if uncertain_files else "known",
            "uncertain_files": uncertain_files,
            "pwrite_errors": (
                first["writers"]["pwrite_errors"]
                + second["writers"]["pwrite_errors"]
            ),
            "fsync_errors": (
                first["writers"]["fsync_errors"]
                + second["writers"]["fsync_errors"]
            ),
        }
        self.verify_mount_bytes(expected)
        self.report["phase"] = "final-stop"
        stopped_two = self.mount.stop(self.redact)
        completion = {
            "clean_unmounts": [stopped_one, stopped_two],
            "restart_restore": first_verify,
        }
        self.report["completion"] = completion
        final_verify = self.verify_bytes("final", expected)
        completion["final_restore"] = final_verify
        completion["final_remote_history"] = self.remote_history(
            time.monotonic() - self.report_started
        )
        completion["sqlite"] = self.sqlite_integrity()
        retry_counts = {
            "pwrite_errors": self.report["durability"]["pwrite_errors"],
            "reader_errors": first["readers"]["errors"] + second["readers"]["errors"],
            "fsync_errors": self.report["durability"]["fsync_errors"],
            "fsync_retry_attempts": (
                first["writers"]["fsync_retry_attempts"]
                + second["writers"]["fsync_retry_attempts"]
            ),
            "fsync_retry_successes": (
                first["writers"]["fsync_retry_successes"]
                + second["writers"]["fsync_retry_successes"]
            ),
            "uncertain_files": uncertain_files,
            "fatal": [*first["fatal"], *second["fatal"]],
            "clean_unmounts": [stopped_one, stopped_two],
            "phases": [
                {
                    "name": name,
                    "fsync_errors": phase["writers"]["fsync_errors"],
                    "fsync_retry_attempts": phase["writers"][
                        "fsync_retry_attempts"
                    ],
                    "fsync_retry_successes": phase["writers"][
                        "fsync_retry_successes"
                    ],
                }
                for name, phase in (
                    ("steady-state-one", first),
                    ("steady-state-two", second),
                )
            ],
        }
        status_command = [
            str(self.yy),
            "--volume",
            str(self.config),
            "status",
        ]
        pre_status = self.yy_run(status_command[1:], timeout=180, check=False)
        acknowledgment = {
            "policy": (
                "acknowledge only persisted ledger entries after every fsync "
                "recovered and all byte, SQLite, and handle-state checks pass"
            ),
            "retry": retry_counts,
            "pre_status": {
                "command": status_command,
                "returncode": pre_status.returncode,
                "stdout": pre_status.stdout,
                "stderr": pre_status.stderr,
            },
            "through": None,
            "command": None,
        }
        completion["acknowledgment"] = acknowledgment
        unsafe = []
        if retry_counts["pwrite_errors"]:
            unsafe.append(f"pwrite_errors={retry_counts['pwrite_errors']}")
        if retry_counts["reader_errors"]:
            unsafe.append(f"reader_errors={retry_counts['reader_errors']}")
        if retry_counts["fatal"]:
            unsafe.append("fatal is not empty")
        if retry_counts["uncertain_files"]:
            unsafe.append(f"uncertain_files={retry_counts['uncertain_files']}")
        for phase in retry_counts["phases"]:
            errors = phase["fsync_errors"]
            attempts = phase["fsync_retry_attempts"]
            successes = phase["fsync_retry_successes"]
            if attempts != errors:
                unsafe.append(
                    f"{phase['name']} fsync retry attempts={attempts} errors={errors}"
                )
            if successes > attempts:
                unsafe.append(
                    f"{phase['name']} fsync retry successes={successes} "
                    f"attempts={attempts}"
                )
            if (successes > 0) != (errors > 0):
                unsafe.append(
                    f"{phase['name']} fsync retry successes={successes} errors={errors}"
                )
        for unmount in retry_counts["clean_unmounts"]:
            if not unmount.get("stopped") or unmount.get("returncode") != 0:
                unsafe.append("mount did not stop cleanly")
                break
        if pre_status.returncode != 0:
            unsafe.append(f"pre-acknowledgment status exit={pre_status.returncode}")
            pre_snapshot = None
        else:
            pre_snapshot = file_status_snapshot(pre_status.stdout)
            acknowledgment["pre_status"]["counts"] = pre_snapshot
            for name in ("pending", "frozen", "conflicts", "failed"):
                if pre_snapshot[name]:
                    unsafe.append(f"pre-acknowledgment {name}={pre_snapshot[name]}")
            sequences = pre_snapshot["sequences"]
            if sequences != sorted(set(sequences)):
                unsafe.append("ledger sequences are not unique and ordered")
            if any(sequence <= 0 for sequence in sequences):
                unsafe.append("ledger contains a non-persisted sequence")
            if any(not error["persisted"] for error in pre_snapshot["errors"]):
                unsafe.append("ledger contains an unpersisted error")
        if unsafe:
            raise EnvelopeError(
                "acknowledgment safety gate failed: " + "; ".join(unsafe)
            )
        if pre_snapshot["ledger_errors"]:
            through = pre_snapshot["sequences"][-1]
            acknowledge_command = [
                str(self.yy),
                "--volume",
                str(self.config),
                "file",
                "acknowledge-errors",
                str(through),
            ]
            acknowledged = self.yy_run(
                acknowledge_command[1:], timeout=180, check=False
            )
            acknowledgment["through"] = through
            acknowledgment["command"] = acknowledge_command
            acknowledgment["result"] = {
                "returncode": acknowledged.returncode,
                "stdout": acknowledged.stdout,
                "stderr": acknowledged.stderr,
            }
            post_status = self.yy_run(status_command[1:], timeout=180, check=False)
            acknowledgment["post_status"] = {
                "command": status_command,
                "returncode": post_status.returncode,
                "stdout": post_status.stdout,
                "stderr": post_status.stderr,
            }
            if acknowledged.returncode != 0:
                unsafe.append(
                    f"acknowledge-errors exit={acknowledged.returncode}"
                )
            if post_status.returncode != 0:
                unsafe.append(
                    f"post-acknowledgment status exit={post_status.returncode}"
                )
            else:
                post_snapshot = file_status_snapshot(post_status.stdout)
                acknowledgment["post_status"]["counts"] = post_snapshot
                for name in (
                    "pending",
                    "frozen",
                    "conflicts",
                    "failed",
                    "ledger_errors",
                ):
                    if post_snapshot[name]:
                        unsafe.append(f"post-acknowledgment {name}={post_snapshot[name]}")
            if unsafe:
                raise EnvelopeError(
                    "acknowledgment did not converge: " + "; ".join(unsafe)
                )
        doctor = self.doctor()
        completion["doctor"] = doctor
        if doctor.get("status") != "pass":
            raise EnvelopeError(f"doctor status is {doctor.get('status')!r}")
        self.report["status"] = "pass"
        self.report["phase"] = "complete"
        self.report["ended_at"] = utc_now()

    def cleanup_success(self) -> None:
        if not self.args.keep_workdir:
            shutil.rmtree(self.work, ignore_errors=True)
            self.report["workdir_retained"] = False
        else:
            self.report["workdir_retained"] = True

    def cleanup_failure(self) -> None:
        self.mount.force_unmount()
        self.report["workdir_retained"] = True


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    yy = Path(args.yy).resolve()
    if not yy.is_file():
        raise EnvelopeError(f"yy binary is unavailable: {yy}")
    source_sha = args.source_sha.strip()
    if len(source_sha) != 40 or any(character not in "0123456789abcdef" for character in source_sha):
        raise EnvelopeError("--source-sha must be a 40-character lowercase Git SHA")
    return {
        "schema": SCHEMA,
        "status": "fail",
        "started_at": utc_now(),
        "source_sha": source_sha,
        "binary": {
            "path": str(yy),
            "sha256": binary_sha256(yy),
        },
        "backend": {
            "label": args.backend_label,
            "profile": os.environ.get("YINYANG_STORAGE_PROFILE"),
            "region": os.environ.get("YINYANG_S3_REGION"),
            "bucket": os.environ.get("YINYANG_S3_BUCKET"),
            "endpoint": "provider-default",
        },
        "host": host_identity(),
        "workload": {
            "duration_seconds": args.duration_seconds,
            "sample_interval_seconds": args.sample_interval,
            "history_interval_seconds": args.history_interval,
            "writers": args.writers,
            "readers": args.readers,
            "write_interval_seconds": args.write_interval,
            "read_interval_seconds": args.read_interval,
            "fsync_retry_seconds": args.fsync_retry_seconds,
            "fsync_retry_interval": args.fsync_retry_interval,
            "restart_at_fraction": 0.5,
        },
        "limits": [
            "one finite sample on one host and one exact binary",
            "not a production throughput, latency, concurrency or capacity claim",
            "whole-file Managed staging can retain a complete new S3 version per write",
            "no automatic garbage collection or retained-history upper bound",
            "optional exact request count requires compatible process-scoped client accounting",
        ],
    }


def atomic_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    bytes_ = json.dumps(document, indent=2, sort_keys=True).encode() + b"\n"
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    try:
        view = memoryview(bytes_)
        while view:
            written = os.write(descriptor, view)
            if written == 0:
                raise EnvelopeError(f"short write to {temporary}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yy", required=True, help="installed yy binary to measure")
    parser.add_argument("--source-sha", required=True, help="40-character source commit")
    parser.add_argument("--backend-label", required=True)
    parser.add_argument("--output", required=True, help="JSON evidence destination")
    parser.add_argument(
        "--work-root",
        default="/var/tmp/yinyang-operating-envelope",
        help="private parent for temporary state",
    )
    parser.add_argument("--duration-seconds", type=int, default=2700)
    parser.add_argument("--sample-interval", type=float, default=5.0)
    parser.add_argument("--history-interval", type=float, default=300.0)
    parser.add_argument("--files", type=int, default=24)
    parser.add_argument("--file-bytes", type=int, default=256 * 1024)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--readers", type=int, default=2)
    parser.add_argument("--write-interval", type=float, default=2.0)
    parser.add_argument("--read-interval", type=float, default=0.5)
    parser.add_argument("--fsync-retry-seconds", type=float, default=30.0)
    parser.add_argument("--fsync-retry-interval", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=20261028)
    parser.add_argument(
        "--request-accounting",
        help=f"optional {REQUEST_SCHEMA} JSON from a compatible client counter",
    )
    parser.add_argument("--keep-workdir", action="store_true")
    args = parser.parse_args()
    if args.duration_seconds < 60:
        parser.error("--duration-seconds must be at least 60")
    if args.sample_interval <= 0 or args.history_interval <= 0:
        parser.error("sample and history intervals must be positive")
    if args.files <= 0 or args.file_bytes <= 0:
        parser.error("files and file bytes must be positive")
    if args.file_bytes % PAGE_BYTES != 0:
        parser.error("--file-bytes must be a multiple of 4096")
    if args.writers <= 0 or args.writers > args.files:
        parser.error("--writers must be between 1 and --files")
    if args.readers < 0:
        parser.error("--readers cannot be negative")
    if args.write_interval <= 0 or args.read_interval <= 0:
        parser.error("write and read intervals must be positive")
    if args.fsync_retry_seconds <= 0 or args.fsync_retry_interval <= 0:
        parser.error("fsync retry window and interval must be positive")
    if args.fsync_retry_interval > FSYNC_RETRY_MAX_INTERVAL:
        parser.error("--fsync-retry-interval exceeds the bounded maximum")
    if Path(args.output).exists():
        parser.error("--output already exists; preserve prior evidence and choose a new path")
    return args


def main() -> int:
    args = parse_args()
    report: dict[str, Any] = {}
    harness: Harness | None = None
    exit_code = 1
    try:
        report = build_report(args)
        harness = Harness(args, report)
        harness.execute()
        harness.cleanup_success()
        exit_code = 0
    except KeyboardInterrupt:
        report["error"] = "interrupted"
    except Exception as error:  # noqa: BLE001 - every failed precondition must reach evidence.
        report["error"] = (
            harness.redact(str(error)) if harness is not None else str(error)
        )
    finally:
        if harness is not None and exit_code != 0:
            harness.cleanup_failure()
        output = Path(args.output).resolve()
        if report:
            report["ended_at"] = utc_now()
            atomic_json(output, report)
        print(
            json.dumps(
                {
                    "schema": report.get("schema"),
                    "status": report.get("status"),
                    "output": str(output),
                    "source_sha": report.get("source_sha"),
                    "binary_sha256": report.get("binary", {}).get("sha256"),
                    "error": report.get("error"),
                },
                sort_keys=True,
            )
        )
    return exit_code


if __name__ == "__main__":
    def interrupt(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    sys.exit(main())
