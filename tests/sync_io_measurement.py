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

"""Task-only Managed Sync local-I/O comparison.

This script is removed before delivery. It measures the release `yy` product
entry with syscall-level file-read attribution rather than treating sampled
`/proc/<pid>/io` values as exact. Each measured command runs under:

    strace -f -yy -s 0 -e trace=read,pread64,readv,preadv

Only successful bytes returned by reads of paths below the measured work
directory are counted. Network bytes and process startup files are excluded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

SIZE = 512 * 1024
COUNTS = [int(value) for value in os.environ.get("COUNTS", "32,256").split(",")]
SYSCALLS = "read|pread64|readv|preadv"
COMPLETE = re.compile(
    rf"^(?:(\d+)\s+)?({SYSCALLS})\((\d+)<([^>]*)>(.*)\)\s+=\s+(-?\d+)(?:\s+.*)?$"
)
UNFINISHED = re.compile(
    rf"^(?:(\d+)\s+)?({SYSCALLS})\((\d+)<([^>]*)>(.*)<unfinished \.\.\.>\s*$"
)
RESUMED = re.compile(
    rf"^(?:(\d+)\s+)?<\.\.\. ({SYSCALLS}) resumed>(.*)\)\s+=\s+(-?\d+)(?:\s+.*)?$"
)
READ_MARKER = re.compile(rf"\b(?:{SYSCALLS})\(")
RESUME_MARKER = re.compile(rf"<\.\.\. (?:{SYSCALLS}) resumed>")


def run(command: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{' '.join(command)} exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def write_tree(root: Path, files: int) -> None:
    for index in range(files):
        directory = root / f"d{index % 4}"
        directory.mkdir(parents=True, exist_ok=True)
        data = bytearray([index % 251]) * SIZE
        data[:8] = index.to_bytes(8, "little")
        (directory / f"f{index}").write_bytes(data)


def write_volume(config: Path, state: Path) -> None:
    config.write_text(
        json.dumps(
            {
                "name": "sync-io",
                "model": "managed",
                "access": "sync",
                "frontend": "library",
                "publication": "object",
                "storage_profile": "minio",
                "staging": str(state),
                "read_only": False,
            }
        )
    )


def tree_digest(root: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    files = 0
    total = 0
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        relative = path.relative_to(root).as_posix()
        data = path.read_bytes()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(data)
        files += 1
        total += len(data)
    return {"files": files, "bytes": total, "sha256": digest.hexdigest()}


def classify(path: str, work: Path, roots: tuple[Path, ...], state: Path) -> str:
    if "/install/" in path:
        return "install"
    if "/artifacts/" in path:
        return "artifacts"
    if any(path == str(root) or path.startswith(f"{root}/") for root in roots):
        return "root"
    if path == str(state) or path.startswith(f"{state}/"):
        if any(marker in path for marker in (".db", ".db-wal", ".db-shm")):
            return "database"
        return "state"
    return "other"


def parse_trace(trace: Path, work: Path, roots: tuple[Path, ...], state: Path) -> dict[str, int]:
    totals = {
        "root": 0,
        "install": 0,
        "artifacts": 0,
        "database": 0,
        "state": 0,
        "other_work": 0,
        "all_work": 0,
    }
    pending: dict[str, tuple[str, str]] = {}
    for line in trace.read_text().splitlines():
        complete = COMPLETE.match(line)
        if complete is not None:
            pid, _, _, path, _, returned = complete.groups()
            if int(returned) > 0 and path.startswith(f"{work}/"):
                add_read(totals, path, int(returned), work, roots, state)
            continue

        unfinished = UNFINISHED.match(line)
        if unfinished is not None:
            pid, syscall, _, path, _ = unfinished.groups()
            pid = pid or "0"
            if pid in pending:
                raise RuntimeError(f"read syscall state collision for pid {pid}: {line}")
            pending[pid] = (syscall, path)
            continue

        resumed = RESUMED.match(line)
        if resumed is not None:
            pid, syscall, _, returned = resumed.groups()
            pid = pid or "0"
            state_entry = pending.pop(pid, None)
            if state_entry is None:
                raise RuntimeError(f"read resumed without an unfinished read for pid {pid}: {line}")
            pending_syscall, path = state_entry
            if pending_syscall != syscall:
                raise RuntimeError(
                    f"read resumed as {syscall} but started as {pending_syscall}: {line}"
                )
            if int(returned) > 0 and path.startswith(f"{work}/"):
                add_read(totals, path, int(returned), work, roots, state)
            continue

        if RESUME_MARKER.search(line) or (
            READ_MARKER.search(line) and f"{work}/" in line
        ):
            raise RuntimeError(f"unparsed read syscall record: {line}")

    if pending:
        raise RuntimeError(f"unfinished read syscalls remain: {pending}")
    return totals


def add_read(
    totals: dict[str, int],
    path: str,
    returned: int,
    work: Path,
    roots: tuple[Path, ...],
    state: Path,
) -> None:
    category = classify(path, work, roots, state)
    if category == "other":
        category = "other_work"
    totals[category] += returned
    totals["all_work"] += returned


def check_parser_fixture() -> None:
    with tempfile.TemporaryDirectory(prefix="yy-sync-io-parser-") as temporary:
        work = Path(temporary)
        root = work / "root"
        state = work / "state"
        trace = work / "interleaved.trace"
        trace.write_text(
            "\n".join(
                [
                    f'101 read(3<{root}/a>, ""..., 4096) = 4096',
                    f'102 read(4<{root}/b>, ""..., 4096 <unfinished ...>',
                    f'101 pread64(5<{state}/install/x>, ""..., 8192, 0) = 8192',
                    f'103 read(6<{root}/c>, ""..., 4096 <unfinished ...>',
                    '102 <... read resumed>""..., 4096) = 4096',
                    '103 <... read resumed>""..., 4096) = -1 EAGAIN (Resource temporarily unavailable)',
                    f'104 readv(7<{state}/app.db>, [{{iov_base="", iov_len=4096}}], 1) = 4096',
                    "105 read(8</var/lib/not-target>, \"\"..., 4096) = 4096",
                ]
            )
            + "\n"
        )
        actual = parse_trace(trace, work, (root,), state)
        expected = {
            "root": 8192,
            "install": 8192,
            "artifacts": 0,
            "database": 4096,
            "state": 0,
            "other_work": 0,
            "all_work": 20480,
        }
        if actual != expected:
            raise RuntimeError(f"parser fixture totals differ: {actual} != {expected}")

        malformed = work / "malformed.trace"
        malformed.write_text(f'101 read(3<{root}/a>, ""..., 4096 = 4096\n')
        try:
            parse_trace(malformed, work, (root,), state)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("parser accepted an unclassified target read")

        unfinished = work / "unfinished.trace"
        unfinished.write_text(f'101 read(3<{root}/a>, ""..., 4096 <unfinished ...>\n')
        try:
            parse_trace(unfinished, work, (root,), state)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("parser accepted an unfinished target read")

        print(
            json.dumps(
                {
                    "schema": "yinyang.sync-io-parser-fixture.v1",
                    "actual": actual,
                    "expected": expected,
                    "unclassified_target_rejected": True,
                    "unfinished_target_rejected": True,
                },
                sort_keys=True,
            )
        )


def loadavg() -> list[float]:
    return [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]]


def measure(
    yy: Path,
    args: list[str],
    *,
    env: dict[str, str],
    trace_path: Path,
    work: Path,
    roots: tuple[Path, ...],
    state: Path,
) -> dict[str, object]:
    command = [
        "strace",
        "-f",
        "-yy",
        "-s",
        "0",
        "-qq",
        "-e",
        "trace=read,pread64,readv,preadv",
        "-o",
        str(trace_path),
        str(yy),
        *args,
    ]
    started = time.perf_counter()
    completed = run(command, cwd=work, env=env)
    elapsed = time.perf_counter() - started
    return {
        "instrumented_elapsed_s": round(elapsed, 6),
        "local_read_bytes": parse_trace(trace_path, work, roots, state),
        "run_stdout": completed.stdout,
        "run_stderr": completed.stderr,
        "trace": str(trace_path),
    }


def status_check(
    yy: Path,
    config: Path,
    env: dict[str, str],
    cwd: Path,
    stage: str,
) -> dict[str, object]:
    result = run(
        [str(yy), "--volume", str(config), "sync", "status", "--json"],
        cwd=cwd,
        env=env,
    )
    document = json.loads(result.stdout)
    summary = document["summary"]
    if not summary["in_sync"]:
        raise RuntimeError(f"{stage} did not reach in_sync: {json.dumps(summary)}")
    return {
        "stdout": result.stdout,
        "summary": summary,
    }


def measure_scale(
    *,
    yy: Path,
    files: int,
    label: str,
    output: Path,
    source_sha: str,
) -> dict[str, object]:
    root_prefix = f"sync-io/{label}/{uuid.uuid4().hex}"
    env = os.environ.copy()
    env["YINYANG_S3_ROOT"] = root_prefix
    env["YINYANG_STORAGE_PROFILE"] = "minio"
    work = output / "work" / f"{files}"
    work.mkdir(parents=True)
    traces = output / "traces" / f"{files}"
    traces.mkdir(parents=True)
    completed = False
    try:
        run([str(yy), "create"], cwd=work, env=env)
        push_root = work / "push"
        pull_root = work / "pull"
        push_state = work / "push-state"
        pull_state = work / "pull-state"
        push_root.mkdir()
        push_config = work / "push.json"
        pull_config = work / "pull.json"
        write_volume(push_config, push_state)
        write_volume(pull_config, pull_state)
        write_tree(push_root, files)
        run(
            [str(yy), "--volume", str(push_config), "sync", "init", "--directory", str(push_root)],
            cwd=work,
            env=env,
        )
        run(
            [str(yy), "--volume", str(pull_config), "sync", "init", "--directory", str(pull_root)],
            cwd=work,
            env=env,
        )

        result: dict[str, object] = {
            "schema": "yinyang.sync-io-evidence.v1",
            "label": label,
            "source_sha": source_sha,
            "yy": str(yy),
            "yy_sha256": hashlib.sha256(yy.read_bytes()).hexdigest(),
            "files": files,
            "file_bytes": SIZE,
            "tree_bytes": files * SIZE,
            "host": os.uname().nodename,
            "loadavg_before": loadavg(),
            "measurement_scope": "Managed object publication through loopback Silo",
            "timing_scope": "strace-instrumented elapsed diagnostic; not a native latency claim",
            "collector": "strace -f -yy -s 0 -e trace=read,pread64,readv,preadv",
        }
        push_initial = measure(
            yy,
            ["--volume", str(push_config), "sync", "run"],
            env=env,
            trace_path=traces / "push-initial.trace",
            work=work,
            roots=(push_root,),
            state=push_state,
        )
        push_initial["status"] = status_check(
            yy,
            push_config,
            env,
            work,
            f"{files}-file initial push",
        )
        result["push_initial"] = push_initial
        pull_initial = measure(
            yy,
            ["--volume", str(pull_config), "sync", "run"],
            env=env,
            trace_path=traces / "pull-initial.trace",
            work=work,
            roots=(pull_root,),
            state=pull_state,
        )
        pull_initial["status"] = status_check(
            yy,
            pull_config,
            env,
            work,
            f"{files}-file initial pull",
        )
        result["pull_initial"] = pull_initial
        time.sleep(1.2)
        noops = []
        for index in range(3):
            noop = measure(
                yy,
                ["--volume", str(pull_config), "sync", "run"],
                env=env,
                trace_path=traces / f"noop-{index + 1}.trace",
                work=work,
                roots=(pull_root,),
                state=pull_state,
            )
            noop["status"] = status_check(
                yy,
                pull_config,
                env,
                work,
                f"{files}-file no-op {index + 1}",
            )
            noops.append(noop)
        result["noop"] = noops

        edited = push_root / "d0" / "f0"
        update = bytearray([0xA5]) * SIZE
        update[:8] = (files + 1).to_bytes(8, "little")
        edited.write_bytes(update)
        push_update = measure(
            yy,
            ["--volume", str(push_config), "sync", "run"],
            env=env,
            trace_path=traces / "push-update.trace",
            work=work,
            roots=(push_root,),
            state=push_state,
        )
        push_update["status"] = status_check(
            yy,
            push_config,
            env,
            work,
            f"{files}-file push update",
        )
        result["push_update"] = push_update
        pull_update = measure(
            yy,
            ["--volume", str(pull_config), "sync", "run"],
            env=env,
            trace_path=traces / "pull-update.trace",
            work=work,
            roots=(pull_root,),
            state=pull_state,
        )
        pull_update["status"] = status_check(
            yy,
            pull_config,
            env,
            work,
            f"{files}-file pull update",
        )
        result["pull_update"] = pull_update

        result["push_in_sync"] = result["push_update"]["status"]["summary"]["in_sync"]
        result["pull_in_sync"] = result["pull_update"]["status"]["summary"]["in_sync"]
        result["push_tree"] = tree_digest(push_root)
        result["pull_tree"] = tree_digest(pull_root)
        result["trees_equal"] = result["push_tree"] == result["pull_tree"]
        result["loadavg_after"] = loadavg()
        if not result["push_in_sync"] or not result["pull_in_sync"] or not result["trees_equal"]:
            raise RuntimeError(f"sync verification failed: {json.dumps(result, indent=2)}")
        completed = True
        return result
    finally:
        if completed:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"retaining failed work directory and traces: {work}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--label")
    parser.add_argument("--check-parser-fixture", action="store_true")
    args = parser.parse_args()

    if args.check_parser_fixture:
        check_parser_fixture()
        return
    if args.output is None or args.label is None:
        parser.error("--output and --label are required for a measurement")

    repo = Path(__file__).resolve().parents[1]
    if os.environ.get("SKIP_BUILD") != "1":
        run(["cargo", "build", "--release", "--locked", "-p", "yinyang", "--bin", "yy"], cwd=repo, env=os.environ.copy())
    yy = repo / "target" / "release" / "yy"
    if not yy.is_file():
        raise SystemExit(f"release yy is missing: {yy}")
    if shutil.which("strace") is None:
        raise SystemExit("strace is required for exact local-read attribution")

    source_sha = run(["git", "rev-parse", "HEAD"], cwd=repo, env=os.environ.copy()).stdout.strip()
    dirty = run(["git", "status", "--porcelain"], cwd=repo, env=os.environ.copy()).stdout.strip()
    if dirty:
        raise SystemExit(f"measurement source is dirty:\n{dirty}")

    args.output.mkdir(parents=True, exist_ok=True)
    results = args.output / f"{args.label}.jsonl"
    if results.exists():
        raise SystemExit(f"output already exists: {results}")
    for files in COUNTS:
        record = measure_scale(
            yy=yy,
            files=files,
            label=args.label,
            output=args.output,
            source_sha=source_sha,
        )
        line = json.dumps(record, sort_keys=True)
        with results.open("a") as output:
            output.write(line + "\n")
        print(line, flush=True)


if __name__ == "__main__":
    main()
