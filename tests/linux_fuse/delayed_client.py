#!/usr/bin/env python3
# Apache-2.0 licensed helper for delayed FUSE completion acceptance.
#
# The parent uses the READY line as a barrier after the client's write is
# accepted, then tells this process to fsync. SIGKILL of this process must not
# cancel the already-sent FUSE request because the adapter replies ENOSYS to
# FUSE_INTERRUPT; the daemon is expected to complete it after service recovery.

import os
import sys


def main() -> None:
    path = sys.argv[1]
    fd = os.open(path, os.O_RDWR)
    payload = b"delayed-payload"
    view = memoryview(payload)
    written = 0
    while written < len(view):
        written += os.write(fd, view[written:])
    print("READY", flush=True)
    line = sys.stdin.readline()
    if line.strip() != "FSYNC":
        raise RuntimeError(f"unexpected parent command: {line!r}")
    os.fsync(fd)
    print("FSYNCED", flush=True)
    os.close(fd)


if __name__ == "__main__":
    main()
