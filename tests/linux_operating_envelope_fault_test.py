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

"""Deterministic fault injection for the soak writer's fsync retry policy."""

from __future__ import annotations

import errno
import importlib.util
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch


def load_envelope():
    path = Path(__file__).with_name("linux_operating_envelope.py")
    spec = importlib.util.spec_from_file_location("linux_operating_envelope", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FsyncRetryFaultTest(unittest.TestCase):
    def make_workload(self, retry_seconds: float) -> tuple[object, object]:
        module = load_envelope()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        mountpoint = Path(temporary.name)
        (mountpoint / "file-0000.bin").write_bytes(b"\0" * module.PAGE_BYTES)
        workload = module.Workload(
            mountpoint=mountpoint,
            expected=[bytearray(module.PAGE_BYTES)],
            writers=1,
            readers=0,
            write_interval=0.01,
            read_interval=0.01,
            seed=1,
            fsync_retry_seconds=retry_seconds,
            fsync_retry_interval=0.005,
        )
        return module, workload

    def test_one_pwrite_then_fsync_retry_succeeds(self) -> None:
        module, workload = self.make_workload(retry_seconds=0.1)
        fsync_attempts = 0
        retry_completed = threading.Event()
        pwrite_calls = 0
        pwrite_descriptor = None
        fsync_descriptors = []

        def fake_pwrite(descriptor: int, data: bytes, offset: int) -> int:
            nonlocal pwrite_calls, pwrite_descriptor
            pwrite_calls += 1
            pwrite_descriptor = descriptor
            self.assertGreater(descriptor, 2)
            self.assertEqual(offset, 0)
            return len(data)

        def fake_fsync(descriptor: int) -> None:
            nonlocal fsync_attempts
            fsync_attempts += 1
            fsync_descriptors.append(descriptor)
            if fsync_attempts == 1:
                workload.stop.set()
                raise OSError(errno.EIO, "injected publication retry")
            retry_completed.set()

        with (
            patch.object(module.os, "pwrite", side_effect=fake_pwrite),
            patch.object(module.os, "fsync", side_effect=fake_fsync),
        ):
            workload.start()
            self.assertTrue(retry_completed.wait(1), "fsync retry did not complete")
            summary = workload.finish()

        self.assertEqual(fsync_attempts, 2)
        self.assertEqual(pwrite_calls, 1)
        self.assertIsNotNone(pwrite_descriptor)
        self.assertTrue(all(fd == pwrite_descriptor for fd in fsync_descriptors))
        self.assertEqual(summary["fatal"], [])
        self.assertEqual(summary["writers"]["fsync_errors"], 1)
        self.assertEqual(summary["writers"]["fsync_retry_attempts"], 1)
        self.assertEqual(summary["writers"]["fsync_retry_successes"], 1)
        self.assertEqual(summary["writers"]["uncertain_files"], [])
        self.assertEqual(summary["first_errors"][0]["operation"], "fsync")
        self.assertEqual(summary["first_errors"][0]["errno"], errno.EIO)

    def test_permanent_fsync_failure_is_fatal(self) -> None:
        module, workload = self.make_workload(retry_seconds=0.02)
        first_fsync = threading.Event()
        fsync_attempts = 0
        pwrite_calls = 0
        pwrite_descriptor = None
        fsync_descriptors = []

        def fake_pwrite(descriptor: int, data: bytes, offset: int) -> int:
            nonlocal pwrite_calls, pwrite_descriptor
            pwrite_calls += 1
            pwrite_descriptor = descriptor
            return len(data)

        def fake_fsync(descriptor: int) -> None:
            nonlocal fsync_attempts
            fsync_attempts += 1
            fsync_descriptors.append(descriptor)
            first_fsync.set()
            raise OSError(errno.EIO, "injected permanent publication failure")

        with (
            patch.object(module.os, "pwrite", side_effect=fake_pwrite),
            patch.object(module.os, "fsync", side_effect=fake_fsync),
        ):
            workload.start()
            self.assertTrue(first_fsync.wait(1), "fsync fault was not reached")
            deadline = time.monotonic() + 1
            while not workload.fatal and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertNotEqual(workload.fatal, [], "writer did not reach its fatal deadline")
            summary = workload.finish()

        self.assertEqual(pwrite_calls, 1)
        self.assertIsNotNone(pwrite_descriptor)
        self.assertTrue(all(fd == pwrite_descriptor for fd in fsync_descriptors))
        self.assertNotEqual(summary["fatal"], [])
        self.assertGreater(summary["writers"]["fsync_errors"], 0)
        self.assertGreater(summary["writers"]["fsync_retry_attempts"], 0)
        self.assertEqual(summary["writers"]["fsync_retry_successes"], 0)
        self.assertEqual(summary["writers"]["uncertain_files"], [0])
        self.assertEqual(summary["first_errors"][0]["operation"], "fsync")
        self.assertEqual(summary["first_errors"][0]["errno"], errno.EIO)

    def test_file_status_snapshot_retains_counts_and_sequences(self) -> None:
        module = load_envelope()
        snapshot = module.file_status_snapshot(
            """\
volume: test model: Managed access: Mount frontend: fuse read-only: false
local handles: 2
handle: 00000000-0000-0000-0000-000000000001 local: 1 remote: 1 revision: 000000000000000000000000 pending: true frozen: false conflict: false
handle error: retryable publication
handle: 00000000-0000-0000-0000-000000000002 local: 0 remote: 0 revision: 000000000000000000000000 pending: false frozen: false conflict: true
error 4 handle 00000000-0000-0000-0000-000000000001 persisted true: retryable publication
"""
        )
        self.assertEqual(snapshot["local_handles"], 2)
        self.assertEqual(snapshot["pending"], 1)
        self.assertEqual(snapshot["frozen"], 0)
        self.assertEqual(snapshot["conflicts"], 1)
        self.assertEqual(snapshot["failed"], 1)
        self.assertEqual(snapshot["ledger_errors"], 1)
        self.assertEqual(snapshot["sequences"], [4])
        self.assertTrue(snapshot["errors"][0]["persisted"])

    def test_file_status_snapshot_rejects_inconsistent_handle_count(self) -> None:
        module = load_envelope()
        with self.assertRaises(module.EnvelopeError):
            module.file_status_snapshot("local handles: 1")

    def test_file_status_snapshot_retains_unpersisted_ledger_error(self) -> None:
        module = load_envelope()
        snapshot = module.file_status_snapshot(
            "local handles: 0\n"
            "error 0 handle 00000000-0000-0000-0000-000000000001 "
            "persisted false: volatile failure\n"
        )
        self.assertEqual(snapshot["sequences"], [0])
        self.assertFalse(snapshot["errors"][0]["persisted"])


if __name__ == "__main__":
    unittest.main()
