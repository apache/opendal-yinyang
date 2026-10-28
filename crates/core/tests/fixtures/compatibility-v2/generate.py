#!/usr/bin/env python3
#
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

"""Generate independent binary fixtures for the YYFILE02 compatibility profile.

This file intentionally does not import the Rust implementation. Run it from
this directory with Python 3.13+ and the PyPI `blake3` package.
"""

from __future__ import annotations

import hashlib
import platform
import sqlite3
import struct
import sys
import unicodedata
from pathlib import Path

try:
    import blake3
except ModuleNotFoundError:
    blake3 = None


HERE = Path(__file__).resolve().parent
CONTENT_CONTEXT = "Apache OpenDAL YinYang content profile 2"
REQUEST_CONTEXT = "Apache OpenDAL YinYang request profile 2"
FILESYSTEM = bytes([0x11]) * 16
OBJECT = bytes([0x22]) * 16
COMMIT = bytes([0x33]) * 16
NONCE = bytes([0x44]) * 16
ROOT = bytes([0x55]) * 16
FILE_NODE = bytes([0x66]) * 16
RECEIPT_DIGEST = bytes([0x77]) * 32


def borsh_string(value: str) -> bytes:
    encoded = value.encode()
    return struct.pack("<I", len(encoded)) + encoded


def borsh_bytes(value: bytes) -> bytes:
    return struct.pack("<I", len(value)) + value


def borsh_vec(values: list[bytes]) -> bytes:
    return struct.pack("<I", len(values)) + b"".join(values)


def borsh_option(value: bytes | None) -> bytes:
    return b"\x00" if value is None else b"\x01" + value


def packed_ref(offset: int, data: bytes) -> bytes:
    return (
        OBJECT
        + struct.pack("<Q", offset)
        + struct.pack("<I", len(data))
        + blake3.blake3(data).digest()
    )


def child_hash(tag: bytes, *parts: bytes) -> bytes:
    hasher = blake3.blake3(derive_key_context=CONTENT_CONTEXT)
    hasher.update(tag)
    for part in parts:
        hasher.update(part)
    return hasher.digest()


def content_id(length: int, root_hash: bytes) -> bytes:
    return child_hash(b"file", struct.pack("<Q", length), root_hash)


def node_bytes(node_id: bytes, membership: int) -> bytes:
    return (
        node_id
        + struct.pack("<Q", 1)
        + b"\x00"
        + b"\x00"
        + struct.pack("<Q", membership)
        + b"\x00"
    )


def index_page(records: list[tuple[bytes, bytes]]) -> bytes:
    encoded = [borsh_bytes(key) + borsh_bytes(value) + b"\x00" for key, value in records]
    return b"YYINDEX2" + b"\x00" + borsh_vec(encoded)


def snapshot(
    roots: list[bytes | None], sequence: int = 0, nonce: bytes = bytes(16)
) -> bytes:
    return (
        b"YYSNAP02"
        + FILESYSTEM
        + ROOT
        + struct.pack("<Q", sequence)
        + nonce
        + b"".join(borsh_option(root) for root in roots)
    )


def head(snapshot_ref: bytes, magic: bytes = b"YYHEAD02") -> bytes:
    body = (
        magic
        + b"\x00"
        + FILESYSTEM
        + ROOT
        + struct.pack("<Q", 0)
        + bytes(16)
        + snapshot_ref
    )
    return body + blake3.blake3(body).digest()


def write(name: str, data: bytes) -> None:
    (HERE / name).write_bytes(data)


def content_fixture() -> tuple[bytes, bytes, bytes]:
    leaves = [bytes([7]) * (64 * 1024), b"x"]
    leaf_positions = [0, 1]
    pack = bytearray()
    data_refs = []
    leaf_hashes = []
    leaf_refs = []
    for position, data in zip(leaf_positions, leaves, strict=True):
        offset = len(pack)
        pack.extend(data)
        data_refs.append(packed_ref(offset, data))
        digest = child_hash(b"leaf", struct.pack("<Q", position), struct.pack("<Q", len(data)), data)
        leaf_hashes.append(digest)
        node = b"YYHASH02\x00" + data_refs[-1]
        offset = len(pack)
        pack.extend(node)
        leaf_refs.append(packed_ref(offset, node))
    left = leaf_hashes[0] + leaf_refs[0]
    right = leaf_hashes[1] + leaf_refs[1]
    root_hash = child_hash(
        b"branch", struct.pack("<Q", 0), struct.pack("<Q", 2), leaf_hashes[0], leaf_hashes[1]
    )
    branch = b"YYHASH02\x01" + left + right
    offset = len(pack)
    pack.extend(branch)
    root_ref = packed_ref(offset, branch)
    descriptor = (
        b"YYFILE02"
        + FILESYSTEM
        + struct.pack("<Q", len(b"".join(leaves)))
        + b"\x01"
        + root_hash
        + root_ref
    )
    return bytes(pack), descriptor, content_id(len(b"".join(leaves)), root_hash)


def content_index_failure_fixture(magic: bytes) -> tuple[bytes, bytes]:
    node = magic + (b"\x00" * 128 if magic == b"YYHASH02" else b"")
    if magic == b"YYHASH02":
        node = magic + b"\x7f" + bytes(128)
    root_hash = bytes([0x99]) * 32
    descriptor = (
        b"YYFILE02"
        + FILESYSTEM
        + struct.pack("<Q", 1)
        + b"\x01"
        + root_hash
        + packed_ref(0, node)
    )
    return node, descriptor


def transaction_fixture(descriptor: bytes, digest_value: bytes) -> tuple[bytes, bytes]:
    logical_length = struct.unpack("<Q", descriptor[24:32])[0]
    mutation_logical = (
        b"\x00"
        + FILE_NODE
        + ROOT
        + borsh_string("file")
        + b"\x01"
        + struct.pack("<Q", logical_length)
        + digest_value
        + b"\x01"
    )
    request = (
        b"YYREQ002"
        + FILESYSTEM
        + borsh_vec([])
        + borsh_vec([mutation_logical])
    )
    request_digest = blake3.blake3(request, derive_key_context=REQUEST_CONTEXT).digest()
    base = struct.pack(">Q", 7) + NONCE
    mutation_plan = (
        b"\x00"
        + FILE_NODE
        + ROOT
        + borsh_string("file")
        + borsh_option(borsh_vec([descriptor]))
        + b"\x01"
    )
    plan = (
        b"YYPLAN01"
        + FILESYSTEM
        + COMMIT
        + base
        + request_digest
        + borsh_vec([])
        + borsh_vec([mutation_plan])
    )
    return plan, request_digest


def object_fixtures() -> None:
    root_node = node_bytes(ROOT, 1)
    author_id = bytes([0x88]) * 16
    old_object = OBJECT
    globals()["OBJECT"] = author_id
    nodes_page = index_page([(ROOT, root_node)])
    globals()["OBJECT"] = old_object

    pack = bytearray()
    snapshot_offset = 0
    template = snapshot([packed_ref(0, nodes_page), None, None, None, None])
    nodes_offset = len(template)
    nodes_ref = packed_ref(nodes_offset, nodes_page)
    descriptor = snapshot([nodes_ref, None, None, None, None])
    assert len(descriptor) == nodes_offset
    pack.extend(descriptor)
    pack.extend(nodes_page)
    snap_ref = packed_ref(snapshot_offset, descriptor)
    write("object-current.pack", bytes(pack))
    write("object-current.head", head(snap_ref))

    unknown_snapshot = b"YYSNAP03"
    write("object-unknown-snapshot.pack", unknown_snapshot)
    write(
        "object-unknown-snapshot.head",
        head(packed_ref(0, unknown_snapshot)),
    )

    unknown_index = b"YYINDEX3"
    template = snapshot([packed_ref(0, unknown_index), None, None, None, None])
    index_offset = len(template)
    descriptor = snapshot(
        [
            packed_ref(index_offset, unknown_index),
            None,
            None,
            None,
            None,
        ]
    )
    assert len(descriptor) == index_offset
    write("object-unknown-index.pack", descriptor + unknown_index)
    write(
        "object-unknown-index.head",
        head(packed_ref(0, descriptor)),
    )

    write(
        "object-unknown-head.bin",
        head(packed_ref(0, b""), magic=b"YYHEAD03"),
    )


def names_fixture() -> None:
    names = [
        "K",
        "Straße",
        "Σίσυφος",
        "İstanbul",
        "ＡＢＣ",
        "é",
    ]
    keys = [unicodedata.normalize("NFC", name.casefold()) for name in names]
    ordered = sorted(zip(names, keys), key=lambda item: item[1].encode())
    rows = ["# input\tfolded\torder", *[
        f"{name}\t{key}\t{index}" for index, (name, key) in enumerate(ordered)
    ]]
    (HERE / "name-keys.tsv").write_text("\n".join(rows) + "\n", encoding="utf-8")
    (HERE / "unicode-version.txt").write_text(
        f"Python unicodedata {unicodedata.unidata_version}\n", encoding="ascii"
    )
    (HERE / "unicode-producer.txt").write_text(
        f"Python {platform.python_version()}, unicodedata {unicodedata.unidata_version}\n",
        encoding="ascii",
    )


def service_fixtures() -> None:
    revision = struct.pack("<Q", 0) + bytes(16)
    root_node = node_bytes(ROOT, 1)
    receipt = COMMIT + RECEIPT_DIGEST + revision + struct.pack("<I", 2)
    write("service-binding-current.bin", b"YYSERV01" + FILESYSTEM + ROOT)
    write(
        "service-binding-unknown.bin",
        b"YYSERV02" + FILESYSTEM + ROOT,
    )
    write(
        "service-binding-malformed.bin",
        b"YYSERV01" + FILESYSTEM + ROOT[:-1],
    )
    write(
        "service-binding-other.bin",
        b"YYSERV01" + bytes([0x12]) * 16 + bytes([0x56]) * 16,
    )

    current = HERE / "service-current.db"
    if current.exists():
        current.unlink()
    connection = sqlite3.connect(current)
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        CREATE TABLE authority(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            profile TEXT NOT NULL,
            filesystem BLOB NOT NULL,
            root BLOB NOT NULL,
            latest BLOB NOT NULL
        );
        CREATE TABLE revisions(token BLOB PRIMARY KEY, sequence INTEGER UNIQUE NOT NULL);
        CREATE TABLE records(
            family INTEGER NOT NULL,
            key BLOB NOT NULL,
            start INTEGER NOT NULL,
            end INTEGER,
            value BLOB NOT NULL,
            PRIMARY KEY(family,key,start)
        );
        CREATE INDEX records_current ON records(family,key) WHERE end IS NULL;
        CREATE TABLE prepared(descriptor BLOB PRIMARY KEY);
        """
    )
    connection.execute(
        "INSERT INTO authority VALUES(1,?,?,?,?)",
        ("yinyang-sqlite-1", FILESYSTEM, ROOT, revision),
    )
    connection.execute("INSERT INTO revisions VALUES(?,0)", (revision,))
    connection.execute(
        "INSERT INTO records VALUES(0,?,0,NULL,?)", (ROOT, root_node)
    )
    connection.execute(
        "INSERT INTO records VALUES(2,?,0,NULL,?)", (COMMIT, receipt)
    )
    connection.commit()
    connection.close()

    unknown = HERE / "service-unknown.db"
    if unknown.exists():
        unknown.unlink()
    connection = sqlite3.connect(unknown)
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        CREATE TABLE authority(
            singleton INTEGER PRIMARY KEY,
            profile TEXT NOT NULL
        );
        CREATE TABLE sentinel(value TEXT NOT NULL);
        """
    )
    connection.execute("INSERT INTO authority VALUES(1,'yinyang-sqlite-999')")
    connection.execute("INSERT INTO sentinel VALUES('unchanged')")
    connection.commit()
    connection.close()


def main() -> None:
    if sys.argv[1:] == ["--names-only"]:
        names_fixture()
        return
    if blake3 is None:
        raise SystemExit("the binary fixture generator requires the PyPI blake3 package")

    pack, descriptor, content_digest = content_fixture()
    write("content.pack", pack)
    write("content.descriptor", descriptor)
    write("content.id", content_digest.hex().encode() + b"\n")

    plan, request_digest = transaction_fixture(descriptor, content_digest)
    write("transaction.plan", plan)
    write("request.digest", request_digest.hex().encode() + b"\n")

    receipt = COMMIT + RECEIPT_DIGEST + struct.pack("<Q", 7) + NONCE + struct.pack("<I", 2)
    write("receipt.bin", receipt)
    write("receipt-malformed.bin", receipt + b"\x00")
    write("content-unknown.descriptor", b"YYFILE03" + bytes(64))
    write("content-malformed.descriptor", descriptor + b"\x00")
    write("index-unknown.bin", b"YYINDEX3")
    write("transaction-unknown.plan", b"YYPLAN02" + bytes(64))
    node, descriptor = content_index_failure_fixture(b"YYHASH03")
    write("content-unknown-index.pack", node)
    write("content-unknown-index.descriptor", descriptor)
    node, descriptor = content_index_failure_fixture(b"YYHASH02")
    write("content-malformed-index.pack", node)
    write("content-malformed-index.descriptor", descriptor)

    object_fixtures()
    service_fixtures()
    (HERE / "binary-producer.txt").write_text(
        f"Python {platform.python_version()}, blake3 {blake3.__version__}, "
        f"SQLite {sqlite3.sqlite_version}\n",
        encoding="ascii",
    )

    checksums = []
    for path in sorted(HERE.iterdir()):
        if path.is_file() and path.name not in {
            "README.md",
            "generate.py",
            "SHA256SUMS",
        }:
            checksums.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}")
    (HERE / "SHA256SUMS").write_text("\n".join(checksums) + "\n", encoding="ascii")


if __name__ == "__main__":
    main()
