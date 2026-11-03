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
COUNTS = [32, 256]
READ = re.compile(
    r"^(?:\d+\s+)?(?:read|pread64|readv|preadv)\(\d+<([^>]*)>.* = (\d+)$"
)


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
    for line in trace.read_text().splitlines():
        match = READ.match(line)
        if match is None:
            continue
        path, returned = match.groups()
        returned = int(returned)
        if returned <= 0:
            continue
        if not path.startswith(f"{work}/"):
            continue
        category = classify(path, work, roots, state)
        if category == "other":
            category = "other_work"
        totals[category] += returned
        totals["all_work"] += returned
    return totals


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
    run(command, cwd=work, env=env)
    elapsed = time.perf_counter() - started
    return {
        "elapsed_s": round(elapsed, 6),
        "local_read_bytes": parse_trace(trace_path, work, roots, state),
        "trace": str(trace_path),
    }


def status(yy: Path, config: Path, env: dict[str, str], cwd: Path) -> dict[str, object]:
    result = run(
        [str(yy), "--volume", str(config), "sync", "status", "--json"],
        cwd=cwd,
        env=env,
    )
    return json.loads(result.stdout)


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
        }
        result["push_initial"] = measure(
            yy,
            ["--volume", str(push_config), "sync", "run"],
            env=env,
            trace_path=traces / "push-initial.trace",
            work=work,
            roots=(push_root,),
            state=push_state,
        )
        result["pull_initial"] = measure(
            yy,
            ["--volume", str(pull_config), "sync", "run"],
            env=env,
            trace_path=traces / "pull-initial.trace",
            work=work,
            roots=(pull_root,),
            state=pull_state,
        )
        time.sleep(1.2)
        noops = []
        for index in range(3):
            noops.append(
                measure(
                    yy,
                    ["--volume", str(pull_config), "sync", "run"],
                    env=env,
                    trace_path=traces / f"noop-{index + 1}.trace",
                    work=work,
                    roots=(pull_root,),
                    state=pull_state,
                )
            )
        result["noop"] = noops

        edited = push_root / "d0" / "f0"
        update = bytearray([0xA5]) * SIZE
        update[:8] = (files + 1).to_bytes(8, "little")
        edited.write_bytes(update)
        result["push_update"] = measure(
            yy,
            ["--volume", str(push_config), "sync", "run"],
            env=env,
            trace_path=traces / "push-update.trace",
            work=work,
            roots=(push_root,),
            state=push_state,
        )
        result["pull_update"] = measure(
            yy,
            ["--volume", str(pull_config), "sync", "run"],
            env=env,
            trace_path=traces / "pull-update.trace",
            work=work,
            roots=(pull_root,),
            state=pull_state,
        )

        push_status = status(yy, push_config, env, work)
        pull_status = status(yy, pull_config, env, work)
        result["push_in_sync"] = push_status["summary"]["in_sync"]
        result["pull_in_sync"] = pull_status["summary"]["in_sync"]
        result["push_tree"] = tree_digest(push_root)
        result["pull_tree"] = tree_digest(pull_root)
        result["trees_equal"] = result["push_tree"] == result["pull_tree"]
        result["loadavg_after"] = loadavg()
        if not result["push_in_sync"] or not result["pull_in_sync"] or not result["trees_equal"]:
            raise RuntimeError(f"sync verification failed: {json.dumps(result, indent=2)}")
        return result
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()

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
